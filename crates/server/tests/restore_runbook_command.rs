//! The restore runbook's reconciliation command must actually RUN (#551).
//!
//! A runbook is executed by an operator who has stopped their writers, mid-incident, by
//! pasting it. So a command in it that does not parse is not a documentation nit — it is a
//! step of the disaster-recovery procedure that fails at the worst possible moment, and the
//! operator's next move is to guess.
//!
//! This is not hypothetical: the command was first written omitting `--failure-domains`, which
//! `require_aligned_topology` rejects outright. Prose is not compiled, so nothing caught it.
//! This test compiles it — it extracts the command from the runbook itself and pushes its
//! topology through the SAME validator the real one-shot runs, so the two cannot drift.

use wyrd_server::cli::{parse_endpoints, require_aligned_topology};

const RUNBOOK: &str = "../../docs/design/architecture/m4-first-deployment-blueprint.md";

/// The `wyrd custodian --reconcile-after-restore …` invocation, lifted out of the runbook with
/// its shell line-continuations folded, and split into argv.
fn documented_command() -> Vec<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(RUNBOOK);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read the runbook at {}: {e}", path.display()));

    // Fold `\`-continued shell lines into one physical line, then find the invocation.
    // Normalise line endings first: on a CRLF checkout the continuation is `\<CR><LF>`, and
    // folding only `\<LF>` would leave the command split across lines — the test would then
    // fail for a reason that has nothing to do with the runbook being wrong.
    let joined = text.replace("\r\n", "\n").replace("\\\n", " ");
    let line = joined
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("wyrd custodian") && l.contains("--reconcile-after-restore"))
        .expect(
            "the runbook must document the post-restore reconciliation command — the restore \
             procedure depends on it, and an operator who cannot find it will skip the step and \
             leave every stranded fragment stranded (#551)",
        );

    line.split_whitespace().map(str::to_string).collect()
}

/// The value of `--name` in the documented command.
fn flag<'a>(argv: &'a [String], name: &str) -> Option<&'a str> {
    let i = argv.iter().position(|a| a == name)?;
    argv.get(i + 1).map(String::as_str)
}

/// THE BUG this test exists for. `cmd_custodian` assembles the fleet through
/// `connect_fleet(.., require_aligned_topology)`, which fabricates NOTHING from argument order:
/// `--ids` and `--failure-domains` are each required to carry exactly one entry per endpoint.
///
/// The documented command must therefore satisfy that validator — otherwise the operator stops
/// their writers, pastes the command the runbook gave them, and gets an argument error instead
/// of the sweep the whole procedure depends on.
#[test]
fn the_runbook_restore_command_satisfies_the_topology_validator_it_will_be_checked_against() {
    let argv = documented_command();

    let endpoints = parse_endpoints(flag(&argv, "--endpoints").expect(
        "the documented command must pass --endpoints: the pass sweeps the D-server fleet, \
             and the one-shot refuses to run without one rather than exit 0 having reconciled \
             nothing",
    ))
    .expect("the documented --endpoints value must parse");

    let ids: Vec<u64> = flag(&argv, "--ids")
        .expect("the documented command must pass --ids — the role never fabricates D-server identity from endpoint order")
        .split(',')
        .map(|s| s.trim().parse().expect("each documented --ids entry must be a u64"))
        .collect();

    let domains: Vec<String> = flag(&argv, "--failure-domains")
        .expect(
            "the documented command must pass --failure-domains — `require_aligned_topology` \
             rejects an --endpoints invocation without one label per endpoint, so a runbook \
             command that omits it dies on an argument error BEFORE reconciling anything",
        )
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    // The real validator, not a restatement of it: if its rule ever changes, this test moves
    // with it and the runbook is forced to keep up.
    require_aligned_topology(endpoints.len(), &ids, &domains).unwrap_or_else(|e| {
        panic!(
            "the runbook's reconciliation command does NOT satisfy the topology validator the \
             one-shot runs it through — an operator pasting it mid-restore gets this error \
             instead of the sweep: {e}"
        )
    });
}

/// ...and the safety rules the command's correctness depends on must be stated where the
/// operator reads them. Both are load-bearing, and neither is discoverable from the command:
///
/// * a PARTIAL fleet makes absence a lie — a D server left out looks like missing fragments,
///   and the pass would report live data as LOST; and
/// * a running WRITER does the same from the other side, committing while the pass reads
///   absence as evidence.
#[test]
fn the_runbook_states_the_two_conditions_that_make_the_pass_safe() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(RUNBOOK);
    let text = std::fs::read_to_string(path).unwrap();

    // The step must come BEFORE writers resume: the pass reads absence as evidence.
    let reconcile = text
        .find("--reconcile-after-restore")
        .expect("the runbook must document the reconciliation command");
    let resume = text
        .find("Resume writers")
        .expect("the runbook must tell the operator when to resume writers");
    assert!(
        reconcile < resume,
        "the runbook must reconcile BEFORE resuming writers — the pass reads a fragment's \
         absence as evidence that nothing references it, and a live writer makes that a lie"
    );

    assert!(
        text.contains("COMPLETE fleet"),
        "the runbook must tell the operator to pass the COMPLETE fleet: a D server left out of \
         --endpoints looks exactly like missing fragments, and the pass would report LIVE DATA \
         AS LOST — the one thing it must never do"
    );
}
