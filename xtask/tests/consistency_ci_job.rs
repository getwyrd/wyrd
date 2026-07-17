//! The standing off-Check CI job for the #408 checked consistency run, and the bug->DST
//! regression loop around it (issue #409, M4 slice 6 of #329; ADR-0009, ADR-0041 §Decision,
//! ADR-0016).
//!
//! Before this bundle the seat `crates/server/src/consistency_workload.rs` names for the
//! routed Elle verdict (`ELLE_OFF_CHECK_VERDICT_JOB = "elle-register-verdict"`) was empty:
//! no workflow ran the #408 checked consistency run on a schedule, `report-nightly-failure.yml`
//! did not watch it, and `crates/dst/tests/commit_ambiguity.rs` had no committed-seed
//! promotion anchor. This file is the Check-time evidence that all three exist and are wired
//! together — five independently-attributable `#[test]` functions, one per deliverable
//! (a)-(e), so a red on one never masks a red on another.
//!
//! Container-free, JVM-free, and offline by design (ADR-0016): every assertion here is a file
//! read + substring/parse, following the `xtask/tests/fdb_image.rs` / `fdb_harness.rs`
//! precedent — the parsing helpers are local to THIS file (nothing imported from `xtask`'s
//! lib, and no `wyrd-server` dependency), so this target stays default-compiled with no
//! feature/cfg gate (`xtask/Cargo.toml` has none today; this file adds none). The workflow's
//! own execution (Docker/FDB cluster/JVM/elle-cli) is deferred to a post-merge
//! `workflow_dispatch` — see the workflow's own header for that confirmation step.

use std::path::{Path, PathBuf};

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

const SEAT_SOURCE: &str = "crates/server/src/consistency_workload.rs";
const WORKFLOW: &str = ".github/workflows/elle-register-verdict.yml";
const REPORTER: &str = ".github/workflows/report-nightly-failure.yml";
const MAIN_RS: &str = "xtask/src/main.rs";
const RUNNER_SRC: &str = "xtask/src/consistency_run_runner.rs";
const SCENARIO_SRC: &str = "crates/server/tests/consistency_run_fdb.rs";
const DST_ANCHOR: &str = "crates/dst/tests/commit_ambiguity.rs";

// ─── pure parsing helpers (local to this test) ────────────────────────────────

/// The quoted value of `pub const <name>: &str = "<value>";` in `src`, if declared.
fn const_str_value(src: &str, name: &str) -> Option<String> {
    let needle = format!("const {name}: &str = \"");
    let idx = src.find(&needle)?;
    let rest = &src[idx + needle.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// The workflow's own top-level `name: <value>` line — column 0, distinct from a job's or a
/// step's `name:` (which are always indented).
fn top_level_name(yaml: &str) -> Option<String> {
    for line in yaml.lines() {
        if let Some(rest) = line.strip_prefix("name:") {
            return Some(rest.trim().to_string());
        }
    }
    None
}

/// The text of the top-level `on:` block: from the `on:` line (exclusive) up to the next
/// column-0 key (`permissions:`, `jobs:`, …), so a trigger pinning check never accidentally
/// matches a job/step body below it.
fn on_block(yaml: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for line in yaml.lines() {
        if line == "on:" {
            inside = true;
            continue;
        }
        if inside {
            if !line.is_empty() && !line.starts_with(' ') && !line.starts_with('\t') {
                break;
            }
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// The `cron: "<min> <hour> * * *"` hour field of a trigger block, if present.
fn cron_hour(block: &str) -> Option<u32> {
    for line in block.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("- cron:") else {
            continue;
        };
        let expr = rest.trim().trim_matches('"');
        let hour = expr.split_whitespace().nth(1)?;
        return hour.parse().ok();
    }
    None
}

/// Every shell line a `run:` key actually executes, inline or block — full-line comments
/// excluded. Mirrors `xtask/tests/fdb_harness.rs`'s `run_script_lines`, kept local to this
/// file per the Verification posture's test-graph constraint (no cross-test-file import).
fn run_script_lines(yaml: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut block_indent: Option<usize> = None;

    for raw in yaml.lines() {
        let trimmed = raw.trim_start();
        let indent = raw.len() - trimmed.len();

        if let Some(key_indent) = block_indent {
            if trimmed.is_empty() {
                continue;
            }
            if indent > key_indent {
                if !trimmed.starts_with('#') {
                    out.push(trimmed.to_string());
                }
                continue;
            }
            block_indent = None;
        }

        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (key_indent, key_line) = match trimmed.strip_prefix("- ") {
            Some(rest) => (indent + 2, rest),
            None => (indent, trimmed),
        };
        let Some(value) = key_line.strip_prefix("run:") else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() || value.starts_with('|') || value.starts_with('>') {
            block_indent = Some(key_indent);
        } else {
            out.push(value.to_string());
        }
    }
    out
}

/// The `cargo xtask <sub>` subcommand at the HEAD of one executed shell line, if any — never
/// a substring scan, so a mention-as-argument or a trailing comment does not count as an
/// execution. Mirrors `xtask/tests/fdb_harness.rs`'s `xtask_head_subcommand`.
fn xtask_head_subcommand(command: &str) -> Option<String> {
    let mut tokens = command.split_whitespace();
    if tokens.next()? != "cargo" || tokens.next()? != "xtask" {
        return None;
    }
    let sub = tokens
        .next()?
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-');
    (!sub.is_empty()).then(|| sub.to_string())
}

/// The `cargo xtask` subcommands the given shell lines invoke as command heads, in order,
/// deduplicated.
fn xtask_head_subcommands(lines: &[String]) -> Vec<String> {
    let mut subs: Vec<String> = Vec::new();
    for line in lines {
        if let Some(sub) = xtask_head_subcommand(line) {
            if !subs.iter().any(|s| s == &sub) {
                subs.push(sub);
            }
        }
    }
    subs
}

/// The subcommands `xtask/src/main.rs` really dispatches, scraped from the `Some("<sub>") =>`
/// arms of its `match task.as_deref()` table.
fn dispatched_subcommands(main_rs: &str) -> Vec<String> {
    let mut subs = Vec::new();
    for line in main_rs.lines() {
        let Some(rest) = line.trim_start().strip_prefix("Some(\"") else {
            continue;
        };
        let Some(end) = rest.find("\")") else {
            continue;
        };
        if rest[end..].contains("=>") {
            subs.push(rest[..end].to_string());
        }
    }
    subs
}

/// The workflow's steps, one string each, comments stripped.
fn steps(yaml: &str) -> Vec<String> {
    let mut steps: Vec<String> = Vec::new();
    let mut current: Option<String> = None;
    for raw in yaml.lines() {
        let trimmed = raw.trim_start();
        if trimmed.starts_with('#') {
            continue;
        }
        if trimmed.starts_with("- name:") || trimmed.starts_with("- uses:") {
            if let Some(step) = current.take() {
                steps.push(step);
            }
            current = Some(String::new());
        }
        if let Some(step) = current.as_mut() {
            step.push_str(raw);
            step.push('\n');
        }
    }
    steps.extend(current);
    steps
}

/// The `workflows:` list nested under the `workflow_run:` trigger — scoped to that block
/// (rather than a whole-file substring) so a match can't come from unrelated prose.
fn watched_workflows_block(yaml: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for line in yaml.lines() {
        let trimmed = line.trim();
        if trimmed == "workflows:" {
            inside = true;
            continue;
        }
        if inside {
            if trimmed.starts_with("- ") {
                out.push_str(trimmed);
                out.push('\n');
                continue;
            }
            break;
        }
    }
    out
}

// ─── (a) the seat coupling: workflow name == the landed verdict-dispatch seat ─────────

#[test]
fn a_workflow_name_equals_the_landed_verdict_dispatch_seat() {
    let src = read(SEAT_SOURCE);
    let seat = const_str_value(&src, "ELLE_OFF_CHECK_VERDICT_JOB").unwrap_or_else(|| {
        panic!("{SEAT_SOURCE} no longer declares `pub const ELLE_OFF_CHECK_VERDICT_JOB`")
    });
    // Sanity: the constant this test binds to still names what the brief says it names.
    assert_eq!(
        seat, "elle-register-verdict",
        "{SEAT_SOURCE}'s ELLE_OFF_CHECK_VERDICT_JOB changed shape — re-derive the expected \
         workflow name from the source, don't hardcode past it"
    );
    // The seat is really the ROUTED default, not a dead constant nobody reads.
    assert!(
        src.contains("consistency_verdict_dispatch") && src.contains("OffCheckElle"),
        "{SEAT_SOURCE} no longer routes the verdict to ELLE_OFF_CHECK_VERDICT_JOB via \
         consistency_verdict_dispatch/OffCheckElle — the seat this workflow fills may no \
         longer be load-bearing"
    );

    let workflow = read(WORKFLOW);
    let name = top_level_name(&workflow)
        .unwrap_or_else(|| panic!("{WORKFLOW} declares no top-level `name:`"));
    assert_eq!(
        name, seat,
        "{WORKFLOW}'s `name:` ({name:?}) must equal ELLE_OFF_CHECK_VERDICT_JOB ({seat:?}) — \
         {SEAT_SOURCE}'s routed seat names the workflow that must fill it"
    );
}

// ─── (b) triggers, timeout, thinness, and the cross-YAML-boundary dispatch coupling ────

#[test]
fn b_triggers_are_schedule_and_dispatch_only_bounded_and_thin() {
    let workflow = read(WORKFLOW);
    let on = on_block(&workflow);

    assert!(
        on.contains("schedule:") && on.contains("cron:"),
        "{WORKFLOW} must trigger on `schedule:`: {on}"
    );
    assert!(
        on.contains("workflow_dispatch:"),
        "{WORKFLOW} must trigger on `workflow_dispatch:`: {on}"
    );
    assert!(
        !on.contains("pull_request"),
        "{WORKFLOW} must NOT trigger on pull_request — a 3-node FDB cluster + JVM checker on \
         every PR contradicts the off-Check privileged-tier boundary (ADR-0041): {on}"
    );
    assert!(
        !on.to_lowercase().contains("push"),
        "{WORKFLOW} must NOT trigger on push: {on}"
    );

    let hour = cron_hour(&on)
        .unwrap_or_else(|| panic!("{WORKFLOW} declares no parseable `cron:` hour: {on}"));
    let occupied = [2, 3, 4, 5, 6]; // tier1-jepsen, tier1-disk-faults, integration-nightly/mutants, tier2-kill-reconstruct, fdb-conformance
    assert!(
        !occupied.contains(&hour),
        "{WORKFLOW}'s cron hour {hour} collides with an existing nightly job's slot \
         {occupied:?}"
    );

    assert!(
        workflow.contains("timeout-minutes:"),
        "{WORKFLOW} must be timeout-bounded, or a hung run drifts to GitHub's 6h default (#150)"
    );

    assert!(
        workflow.contains("WYRD_TIER1: \"1\""),
        "{WORKFLOW} must opt in to the #408 runner via WYRD_TIER1=1"
    );

    // Thin per the CI rule (ADR-0009 §CI): the workflow's ONLY `cargo xtask` invocation is
    // the #408 runner — no other CI logic rides along as a second xtask subcommand.
    let lines = run_script_lines(&workflow);
    let heads = xtask_head_subcommands(&lines);
    assert_eq!(
        heads,
        vec!["consistency-run".to_string()],
        "{WORKFLOW} must invoke exactly `cargo xtask consistency-run` as its only xtask head \
         — carrying CI logic in YAML (a second xtask subcommand, or a bare `cargo test`) \
         violates the CI-logic-lives-in-xtask rule: heads={heads:?}"
    );

    // The cross-YAML-boundary coupling: the invoked subcommand must be a REAL dispatch arm of
    // xtask/src/main.rs, so a typo'd or later-renamed subcommand fails here instead of on the
    // first cron.
    let main_rs = read(MAIN_RS);
    let dispatched = dispatched_subcommands(&main_rs);
    assert!(
        dispatched.contains(&"ci".to_string()) && dispatched.len() > 3,
        "the dispatch-table scrape of {MAIN_RS} found suspiciously few `Some(\"…\") =>` arms — \
         the cross-check would be vacuous: {dispatched:?}"
    );
    assert!(
        dispatched.contains(&"consistency-run".to_string()),
        "{WORKFLOW} runs `cargo xtask consistency-run`, but {MAIN_RS} dispatches no such \
         subcommand (dispatched: {dispatched:?}) — the #408 runner is not wired, or was \
         renamed"
    );
}

// ─── (c) the anomaly raw material is uploaded from where #408 actually writes it ───────

#[test]
fn c_the_run_artifacts_are_uploaded_from_where_408_actually_writes_them() {
    // Bind to what #408's folded runner ACTUALLY emits — read it from the wave base, never
    // assert a guessed path (the brief's explicit instruction: the design doc's paths are
    // provisional until 408's fold).
    let runner_src = read(RUNNER_SRC);
    assert!(
        runner_src.contains("workspace_root().join(\"target/consistency-run\")"),
        "{RUNNER_SRC}'s output_dir() no longer resolves to target/consistency-run — the \
         artifact path binding below is stale"
    );
    assert!(
        runner_src.contains("\"run-summary.json\""),
        "{RUNNER_SRC} no longer reads run-summary.json from the output dir"
    );
    assert!(
        runner_src.contains("\"report.md\""),
        "{RUNNER_SRC} no longer writes report.md to the output dir"
    );
    let scenario_src = read(SCENARIO_SRC);
    for emitted in [
        "register-history.edn",
        "directory-history.edn",
        "run-summary.json",
    ] {
        assert!(
            scenario_src.contains(emitted),
            "{SCENARIO_SRC} no longer writes {emitted} under WYRD_CONSISTENCY_OUTPUT_DIR — the \
             artifact this workflow uploads no longer exists"
        );
    }

    let workflow = read(WORKFLOW);
    let upload_steps: Vec<String> = steps(&workflow)
        .into_iter()
        .filter(|s| s.contains("upload-artifact"))
        .collect();
    assert_eq!(
        upload_steps.len(),
        1,
        "{WORKFLOW} must have exactly one upload-artifact step, found {}",
        upload_steps.len()
    );
    let upload = &upload_steps[0];
    assert!(
        upload.contains("target/consistency-run"),
        "{WORKFLOW}'s upload-artifact step does not reference target/consistency-run — the \
         directory {RUNNER_SRC} and {SCENARIO_SRC} actually write the EDN histories, the run \
         summary, and the report into: {upload}"
    );
    assert!(
        !upload.contains("if: failure()"),
        "{WORKFLOW}'s upload-artifact step must not be gated on failure() alone — the anomaly \
         raw material (EDN histories, checker output, report) is needed from a passing run \
         too, so a later anomalous run has a clean baseline to diff against: {upload}"
    );
}

// ─── (d) the learning loop's entry: the nightly reporter watches the new job ───────────

#[test]
fn d_the_nightly_reporter_watches_the_new_job() {
    let reporter = read(REPORTER);
    let watched = watched_workflows_block(&reporter);
    assert!(
        watched.contains("- elle-register-verdict"),
        "{REPORTER}'s watched `workflows:` list omits elle-register-verdict — a scheduled \
         failure of the new job would produce no tracked bug: {watched}"
    );
    // Non-vacuity: the existing watched jobs are still there (the block really is the
    // `workflow_run.workflows` list, not some unrelated `workflows:` mention).
    for existing in [
        "tier1-disk-faults",
        "tier2-kill-reconstruct",
        "integration-nightly",
        "mutants",
    ] {
        assert!(
            watched.contains(existing),
            "{REPORTER}'s watched list lost `{existing}` — {watched}"
        );
    }
    // The reporter's conclusion filter must not be widened to accommodate this job (a
    // timeout-minutes overrun already surfaces as `failure()` on GitHub, per the brief).
    assert!(
        reporter.contains("conclusion == 'failure'"),
        "{REPORTER} must keep firing only on conclusion == 'failure'"
    );
    assert!(
        reporter.contains("event == 'schedule'"),
        "{REPORTER} must keep firing only for scheduled runs"
    );
}

// ─── (e) the DST promotion anchor: a committed-seed replay block exists ────────────────

#[test]
fn e_the_dst_promotion_anchor_exists_and_replays_the_faithful_property_bodies() {
    let dst = read(DST_ANCHOR);

    assert!(
        dst.contains("const REGRESSION_SEEDS: &[u64]"),
        "{DST_ANCHOR} has no committed REGRESSION_SEEDS anchor (contrast \
         crates/dst/tests/custodian.rs:1351)"
    );
    assert!(
        dst.contains("fn committed_regression_seeds_stay_green"),
        "{DST_ANCHOR} has no committed_regression_seeds_stay_green replay test"
    );

    // The replay drives the SAME production property bodies the campaign sweeps above use —
    // never a copy or a permissive twin — in their faithful configuration.
    for body in [
        "run_cas_ambiguity(",
        "run_blind_ambiguity(",
        "run_timeout_ambiguity(",
        "run_contended_1031(",
    ] {
        assert!(
            dst.matches(body).count() >= 2,
            "{DST_ANCHOR}'s anchor must call `{body}` (once for the existing sweep, once for \
             the committed-seed replay) — found {} occurrence(s)",
            dst.matches(body).count()
        );
    }
    for faithful in [
        "FdbFidelity::CommitUnknownResult",
        "Observer::SettlingReRead",
        "BlindObserver::SettlingReRead",
        "TimeoutObserver::AcceptsIndeterminacy",
        "ContendedObserver::SettleThenReRead",
    ] {
        assert!(
            dst.contains(faithful),
            "{DST_ANCHOR}'s anchor must replay in the FAITHFUL configuration `{faithful}` — a \
             permissive twin would replay green without exercising the real property"
        );
    }

    // Do NOT port custodian's macro/RNG machinery (the brief's explicit instruction) — the
    // replay mechanics here are this file's own plain-#[test] idiom. Matched as an
    // INVOCATION/USE, not a bare mention, so this test's own prose (which names what it is
    // NOT porting) can't trip its own assertion.
    assert!(
        !dst.contains("dst_campaign_test! {"),
        "{DST_ANCHOR} must not INVOKE custodian.rs's dst_campaign_test! macro"
    );
    assert!(
        !dst.contains("ChaCha8Rng::seed_from_u64"),
        "{DST_ANCHOR} must not USE custodian.rs's ChaCha8Rng seeding machinery"
    );

    // Anti-vacuity: the replay must assert non-zero ambiguity/deferral counts per seed, the
    // same shape the campaign's own sweep tests carry (commit_ambiguity.rs:332-333,
    // :686-699, :865-896) — never a bare call with no observation asserted.
    assert!(
        dst.contains("ambiguous_conditional_commits >= 1")
            || dst.contains("ambiguous_conditional_commits, "),
        "{DST_ANCHOR}'s anchor asserts no anti-vacuity counter on the CAS/contended legs — a \
         seed that never arms the nemesis could replay green silently"
    );
    assert!(
        dst.contains("ambiguous_blind_commits"),
        "{DST_ANCHOR}'s anchor asserts no anti-vacuity counter on the blind leg"
    );

    // Seed provenance stated honestly (ADR-0009 commits *bug-finding* seeds — never fabricate
    // provenance for a set that never found anything). Comment prose wraps across lines, so
    // normalize by stripping `//` markers and joining with spaces before the phrase search.
    let normalized_prose = dst
        .lines()
        .map(|l| l.trim_start().trim_start_matches("//").trim())
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    assert!(
        normalized_prose.contains("not yet bug-finding"),
        "{DST_ANCHOR}'s anchor must document that its initial seed set is NOT yet bug-finding"
    );
    assert!(
        dst.to_lowercase().contains("minimize"),
        "{DST_ANCHOR}'s anchor must document the minimize-and-append promotion procedure"
    );
}
