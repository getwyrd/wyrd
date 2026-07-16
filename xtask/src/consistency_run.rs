//! **The checked consistency run + public credibility artifact** (#408, slice 5 of #329;
//! ADR-0041). The **host-independent** run-orchestration plan, the summary-based inconclusive
//! gate, the pinned elle-cli invocation builder, the verdict-token parser, the report renderer,
//! and the opted-in-but-missing-environment error paths — all pure functions with no privileged
//! I/O (no `docker`, no `java`, no `libfdb_c`), mirroring [`crate::metadata_faults`] and
//! [`crate::nemesis`]: the privileged live run is deferred / off-Check (`WYRD_TIER1=1`), but the
//! *decisions* it turns on are pure and unit-tested inside the unprivileged `cargo xtask ci`
//! gate — the "deferred ≠ unbuilt" bar.
//!
//! # The seam (Design §1/§2)
//!
//! The **run summary** is the seam between the server-side live scenario
//! (`crates/server/tests/consistency_run_fdb.rs`, feature-gated `fdb`, off-Check) and this
//! module: the scenario writes a JSON summary carrying the #406 INV-2 concurrency witness
//! ([`consistency_workload::MultiProcessHistory::is_genuinely_concurrent`], not importable
//! here) and the **#407 typed materialization evidence** ([`NemesisEvidence`] — the leg's own
//! `MaterializationEvidence::{kind, materialized, diagnosis}`, NOT a hard-coded boolean), and
//! this module derives inconclusive/verdict/report from the summary + checker output alone —
//! never from `wyrd-server` types. That is exactly what keeps the gate arithmetic testable at
//! Check without `xtask` gaining a `wyrd-server`/FDB/JVM dependency (Design §1).
//!
//! # The checker contract, pinned (Design §3 — verified against elle-cli 0.1.9 at Plan)
//!
//! `java -jar $WYRD_ELLE_CLI_JAR --model rw-register <register.edn>` /
//! `--model set <directory.edn>`. The per-file output line is `<file>\t<token>`; the verdict is
//! the **token**, never the exit code alone (verified: `:unknown` exits 0): only the literal
//! `true` is a pass, `false` is a violation (run FAILURE), and `:unknown` (or anything
//! unparsable) is **INCONCLUSIVE** — never a silent pass ([`parse_checker_output`]).
//!
//! # Non-vacuity is a gate, not a note (Design §4)
//!
//! [`evaluate_summary`] refuses a verdict unless the run summary attests BOTH a genuinely
//! concurrent history (INV-2, bound to the Elle-fed register pool) AND a materialized nemesis
//! fault — the #250 failure mode (a vacuous history slipping through) this issue exists to bury.
//! The fault half reads the **leg's own typed evidence** ([`NemesisEvidence::materialized`]), so
//! it can never pass on a boolean the scenario merely asserted from `drive_leg`'s contract.
//!
//! # What is default-compiled vs deferred
//!
//! Everything here is an ordinary `std` + `serde_json` module with **no** `wyrd-server`, FDB,
//! Docker, Java, or elle-cli dependency, so it compiles unconditionally and is imported by
//! `xtask/tests/consistency_run_orchestration.rs` (the Check-time flippable coverage) AND by
//! the impure runner (`xtask/src/consistency_run_runner.rs`, wired from `main.rs`, which does
//! the actual bring-up / shell-out / JVM invocation, off-Check).

use crate::nemesis::NemesisLegKind;

// ─── Run-orchestration plan (Design §1/§2) ─────────────────────────────────────────────

/// The ordered stages one checked consistency run passes through: bring the cluster up, drive
/// the #406 workload, open the #407 nemesis window (enclosing the workload, per
/// `nemesis::drive_leg`), heal the fault, export the Elle-EDN histories + run summary, obtain
/// the checker verdict, and render the report. A stage dropped from this list is a pipeline
/// step the runner silently stopped performing — [`run_plan`] is what
/// `consistency_run_orchestration.rs` pins so that regression flips red, not merely absent.
pub const RUN_STAGES: [&str; 7] = [
    "bring-up",
    "workload",
    "nemesis-window",
    "heal",
    "export",
    "check",
    "report",
];

/// The run-orchestration plan, in order (Design §1/§2). Pure — a plain slice the orchestration
/// unit test inspects directly.
#[must_use]
pub fn run_plan() -> &'static [&'static str] {
    &RUN_STAGES
}

/// The in-repo fdb-feature checked-consistency scenario test binary (the `cargo test --test
/// <name>` target), launched by the runner exactly as the FDB fault battery launches its
/// scenarios (Design §1: `xtask` shells out to `cargo test -p wyrd-server --features fdb`, so
/// `xtask` itself gains no `wyrd-server`/FDB/JVM dependency).
pub const CONSISTENCY_RUN_SCENARIO_TEST: &str = "consistency_run_fdb";

/// The `cargo test -p wyrd-server --features fdb --test <name> -- --ignored --nocapture` argv
/// that runs the live checked-consistency scenario. Mirrors
/// [`crate::metadata_faults::metadata_scenario_args`] / `fdb_faults::run_leg`'s literal argv —
/// the scenario drives the real `FdbMetadataStore` behind the unchanged trait, so the argv MUST
/// carry `--features fdb` (off by default, so the whole-tree gate compiles the empty skeleton).
#[must_use]
pub fn consistency_run_scenario_args(test: &str) -> [&str; 10] {
    [
        "test",
        "-p",
        "wyrd-server",
        "--features",
        "fdb",
        "--test",
        test,
        "--",
        "--ignored",
        "--nocapture",
    ]
}

// ─── Nemesis-leg selection (Design §2: "partition first; skew/pause selectable") ───────

/// Resolve which #407 nemesis leg the checked run injects, from the `WYRD_CONSISTENCY_NEMESIS`
/// env value (or its absence). **Partition is the default** (Design §2), never a silent "no
/// fault" — an unrecognized value is a hard configuration error, not a fallback, so a typo'd
/// leg name cannot silently downgrade the run to an easier fault. Reuses
/// [`crate::nemesis::NemesisLegKind`] rather than re-deriving a second leg enum (the seam is
/// ONE importable lifecycle, Design §1) — #408 selects among the SAME three legs #407 defined,
/// it does not invent a fourth.
///
/// # Errors
/// An unrecognized leg name.
pub fn selected_leg(raw: Option<&str>) -> Result<NemesisLegKind, String> {
    match raw.map(str::trim) {
        None | Some("") | Some("partition") | Some("network-partition") => {
            Ok(NemesisLegKind::NetworkPartition)
        }
        Some("clock-skew") => Ok(NemesisLegKind::ClockSkew),
        Some("process-pause") => Ok(NemesisLegKind::ProcessPause),
        Some(other) => Err(format!(
            "unknown WYRD_CONSISTENCY_NEMESIS leg `{other}` (expected `partition` [default], \
             `clock-skew`, or `process-pause`)"
        )),
    }
}

// ─── The #407 typed materialization evidence — carried, not asserted (Design §4, T2) ───

/// The **#407 typed materialization evidence**, propagated through the run summary and into the
/// report — NOT a hard-coded boolean. The live scenario builds this from the leg's OWN
/// `MaterializationEvidence` (`wyrd_metadata_fault_conformance::nemesis`): `kind()` names the
/// fault class, `materialized()` is the per-leg oracle's verdict over its sampled observations,
/// and `diagnosis()` records HOW the fault provably bit (a partition's before/during
/// reachability flip, a pause's `paused` inspect state, a skew's observed offset vs floor). So
/// the credibility artifact attests *what the leg actually observed*, not that the scenario
/// trusted `drive_leg`'s contract.
///
/// `deny_unknown_fields` for the reason [`RunSummary`] carries it — and **not** by inheritance:
/// serde applies the attribute per struct, so a `RunSummary` that denies unknown fields still
/// silently swallows one added inside its nested `nemesis` object. The seam has to fail loudly at
/// every level it has, or "the seam fails loudly" is only true of the top one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NemesisEvidence {
    /// The fault class, as `NemesisLegKind::as_str` (`partition` / `clock-skew` /
    /// `process-pause`).
    pub kind: String,
    /// Which cluster node the fault hit (service + container + address), so the report names
    /// the target, not merely the fault class.
    pub target: String,
    /// The leg oracle's verdict — `MaterializationEvidence::materialized()`. `false` ⇒ the run
    /// is inconclusive (Design §4, the #442 "a note is not a gate" rule); the gate reads THIS,
    /// never a boolean the scenario merely asserted.
    pub materialized: bool,
    /// The leg's own `diagnosis()` — the sampled observations that prove (or disprove) the
    /// fault bit. Rendered verbatim into the report so an outsider can inspect how the fault
    /// materialized.
    pub diagnosis: String,
}

// ─── The run summary — the seam (Design §2/§4) ─────────────────────────────────────────

/// Per-op-kind outcome counts for one workload pool (Design §4): so a degenerate workload (all
/// failures, no successes) is visible in the summary and the report, never hidden behind a bare
/// op total.
///
/// `deny_unknown_fields` per [`NemesisEvidence`]'s note — serde does not propagate it from the
/// enclosing struct, and this one is nested three deep (`delete_pool.outcomes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeCounts {
    /// Ops invoked.
    pub invoked: usize,
    /// Determinate successful completions.
    pub ok: usize,
    /// Determinate failed completions.
    pub fail: usize,
    /// Indeterminate completions (`:info`).
    pub info: usize,
}

/// One directory member's name↔id mapping (Design §2/§6). Elle's `set` checker takes **integer**
/// elements only, so the wire object name never enters the EDN — which makes this map the only
/// thing that lets a reader of the report tie an element in the checked history back to the object
/// the run actually created. It therefore has to reach the report, not merely the summary file.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberId {
    /// The object name the run created on the wire.
    pub member: String,
    /// The integer element that name is represented by in the `set` history.
    pub id: u64,
}

/// The **Wyrd-checked delete pool**'s verdicts (Design §2): the landed #406 checks are what judge
/// the PUT/GET/DELETE traffic the `rw-register` model cannot represent, so the pool is judged, not
/// merely driven. Each is INV-1-sound — indeterminate ops are skipped rather than turned into a
/// definite obligation — so a `false` is a real violation observed on the live cluster, and the
/// runner fails the run on it exactly as it fails on Elle's `false` ([`wyrd_check_violations`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletePoolChecks {
    /// `MultiProcessHistory::session_read_your_writes` — carries the resurrection / lost-write
    /// logic (a delete that came back, a write that vanished).
    pub session_read_your_writes: bool,
    /// `MultiProcessHistory::session_monotonic_reads`.
    pub session_monotonic_reads: bool,
    /// `MultiProcessHistory::reads_monotone_per_key`.
    pub reads_monotone_per_key: bool,
}

/// The Wyrd-checked delete pool's half of the summary (Design §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletePoolSummary {
    /// Ops recorded on the disjoint delete-pool key set.
    pub ops: usize,
    /// Per-op-kind outcome counts, so a degenerate delete pool is visible (Design §4).
    pub outcomes: OutcomeCounts,
    /// The #406 checks' verdicts over the pool.
    pub checks: DeletePoolChecks,
}

/// The machine-readable run summary the live scenario writes under
/// `target/consistency-run/run-summary.json` — the seam between the server-side scenario and
/// this xtask runner (Design §2). Carries exactly what the non-vacuity gate and the report need:
/// the #406 INV-2 witness, the **#407 typed materialization evidence** ([`NemesisEvidence`]),
/// the per-model history sizes, the per-pool per-op-kind outcome counts (Design §4), the
/// Wyrd-checked delete pool's verdicts, and the member-id map the report resolves elements with.
///
/// **`deny_unknown_fields` is load-bearing.** This struct IS the seam's contract, and serde's
/// default is to ignore a field it does not know — which is silent data loss in the one direction
/// that matters: the scenario emits something the report then never shows, and nothing anywhere
/// fails. (That is exactly how `member_id_map` came to be written into every summary and dropped
/// on the floor.) Denying unknown fields makes the seam fail loudly instead: a field the scenario
/// emits and this struct does not name is a hard parse error, so the two sides cannot drift apart
/// quietly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSummary {
    /// A human-readable description of the workload driven (report field 1).
    pub workload: String,
    /// The #407 nemesis leg's typed materialization evidence (report field 2).
    pub nemesis: NemesisEvidence,
    /// The #406 INV-2 witness: `MultiProcessHistory::is_genuinely_concurrent()` over the merged
    /// **Elle-fed register overwrite pool** (Design §2 — the pool the verdict is claimed on).
    /// `false` ⇒ the run is inconclusive (Design §4).
    pub genuinely_concurrent: bool,
    /// Register (Elle-fed overwrite pool) history op count (report field 3, register half).
    pub register_ops: usize,
    /// Directory (`set` model) history op count (report field 3, directory half).
    pub directory_ops: usize,
    /// Per-op-kind outcome counts for the Elle-fed register overwrite pool (Design §4).
    pub register_outcomes: OutcomeCounts,
    /// Per-op-kind outcome counts for the directory create pool (Design §4).
    pub directory_outcomes: OutcomeCounts,
    /// The Wyrd-checked delete pool: counts + the #406 checks' verdicts (Design §2).
    pub delete_pool: DeletePoolSummary,
    /// The directory member name↔id map (Design §2/§6) — rendered into the report.
    pub member_id_map: Vec<MemberId>,
    /// The integer elements the composed post-heal full-set `:read` observed present (Design §2).
    pub composed_final_read: Vec<u64>,
    /// Whether the post-heal sweep resolved **every** member, so the composed `:read` is a definite
    /// claim about the whole set. `false` ⇒ the scenario emitted the composed read as `:info` and
    /// the directory model can yield no verdict (Design §2) — the run is inconclusive.
    pub composed_final_read_determinate: bool,
    /// The elements the sweep could not resolve (empty iff
    /// [`composed_final_read_determinate`](Self::composed_final_read_determinate)) — named, so the
    /// report says *which* members left the run inconclusive rather than merely that it was.
    pub composed_final_read_unresolved: Vec<u64>,
}

/// Parse a run summary from its JSON text. Malformed JSON (a missing field, a type mismatch) is
/// a hard error — never a default-filled summary that could accidentally read as conclusive.
///
/// # Errors
/// The JSON is malformed or missing a required field.
pub fn parse_run_summary(json: &str) -> Result<RunSummary, String> {
    serde_json::from_str(json).map_err(|e| format!("malformed run summary JSON: {e}"))
}

/// Why a run is **inconclusive** (Design §4) — non-vacuity is a gate, not a note. Both variants
/// are independent (checked in order, but either alone is sufficient to block a verdict).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InconclusiveReason {
    /// The #406 INV-2 witness did not hold: no same-key read↔write overlap across distinct
    /// processes on the Elle-fed register pool, so the history proves nothing about concurrent
    /// behavior (the #250 failure mode).
    NotGenuinelyConcurrent,
    /// The #407 nemesis leg's typed evidence did not attest a materialized fault — the run never
    /// actually exercised a fault, so a verdict over it would be a "note", not a gate.
    NemesisNotMaterialized,
    /// The post-heal sweep could not resolve every directory member, so the composed full-set
    /// `:read` is `:info` and the `set` model has no definite final read to judge.
    ///
    /// This is a *legibility* gate rather than a safety one: the real elle-cli independently
    /// returns `:unknown` for a `set` history whose final read is `:info` (verified against 0.1.9),
    /// which the pinned parser already maps to INCONCLUSIVE — so the run could never pass on it
    /// regardless. Deciding it from the summary means the run says WHICH members were unresolved,
    /// instead of surfacing a bare `:unknown` an operator then has to reverse-engineer.
    FinalReadIndeterminate,
}

impl InconclusiveReason {
    /// A human-readable diagnosis for the report / stderr.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            InconclusiveReason::NotGenuinelyConcurrent => {
                "the run summary does not attest a genuinely concurrent history (#406 INV-2 \
                 witness on the Elle-fed register pool) — a vacuous history proves nothing, \
                 refusing to report a verdict"
            }
            InconclusiveReason::NemesisNotMaterialized => {
                "the run summary's #407 typed evidence does not attest a materialized nemesis \
                 fault — a fault that never bit proves nothing, refusing to report a verdict"
            }
            InconclusiveReason::FinalReadIndeterminate => {
                "the post-heal sweep could not resolve every directory member, so the composed \
                 full-set read is :info and the `set` model has no definite final read to judge — \
                 refusing to report a directory verdict (the alternative, dropping the unresolved \
                 members from a definite :ok read, would fabricate a lost element)"
            }
        }
    }
}

/// **The non-vacuity gate (Design §4).** Before any verdict is reported, the run summary MUST
/// attest BOTH: (a) the INV-2 concurrency witness held on the Elle-fed register pool, and (b) the
/// #407 typed evidence says the nemesis fault materialized. Failing either makes the run
/// **inconclusive** — this is xtask-side arithmetic over the summary alone, never a re-derivation
/// of the checker's own linearizability decision.
///
/// It also requires (c) a determinate composed final read
/// ([`InconclusiveReason::FinalReadIndeterminate`]). That is not a third non-vacuity condition so
/// much as the same refusal to overstate: without it the `set` model has no final read to judge.
/// The checker enforces (c) on its own (`:info` read ⇒ `:unknown` ⇒ inconclusive); deciding it here
/// buys the *diagnosis*, naming the unresolved members.
///
/// # Errors
/// [`InconclusiveReason`] naming which attestation is missing.
pub fn evaluate_summary(summary: &RunSummary) -> Result<(), InconclusiveReason> {
    if !summary.genuinely_concurrent {
        return Err(InconclusiveReason::NotGenuinelyConcurrent);
    }
    if !summary.nemesis.materialized {
        return Err(InconclusiveReason::NemesisNotMaterialized);
    }
    if !summary.composed_final_read_determinate {
        return Err(InconclusiveReason::FinalReadIndeterminate);
    }
    Ok(())
}

/// The **Wyrd-checked delete pool's verdict** (Design §2): the names of every #406 check the pool
/// violated, empty when all of them held. The delete pool exists because the `rw-register` model
/// cannot represent a delete — so the landed INV-1-sound checks are the only thing judging that
/// traffic, and the runner fails the run on a violation here exactly as it does on Elle's `false`.
/// Driving a pool and then not acting on its judgment would make the pool decorative, and a run
/// report listing delete traffic nobody checked overstates precisely what this issue exists to
/// stop overstating.
///
/// Pure arithmetic over the summary — no re-derivation of the checks themselves (they ran
/// server-side, where the history's types live).
#[must_use]
pub fn wyrd_check_violations(summary: &RunSummary) -> Vec<&'static str> {
    let checks = &summary.delete_pool.checks;
    let mut violated = Vec::new();
    if !checks.session_read_your_writes {
        violated.push("session_read_your_writes");
    }
    if !checks.session_monotonic_reads {
        violated.push("session_monotonic_reads");
    }
    if !checks.reads_monotone_per_key {
        violated.push("reads_monotone_per_key");
    }
    violated
}

// ─── The checker contract, pinned (Design §3) ──────────────────────────────────────────

/// The register-history model name (`--model rw-register`), for the main leg (Design §3, ADR-0041
/// §Decision 1).
pub const MODEL_REGISTER: &str = "rw-register";
/// The directory-as-set history model name (`--model set`), for the secondary leg (Design §3).
/// **`set`, not `set-full`** — the v2 `set-full` pin was falsified by the real checker; `set`
/// with a post-heal composed final read is the model that states what the run actually observes.
pub const MODEL_DIRECTORY_SET: &str = "set";

/// Build the `java -jar $WYRD_ELLE_CLI_JAR --model <model> <history_path>` argv (Design §3) —
/// pure so the pinned invocation shape is unit-tested without a JVM. The runner passes this to
/// `Command::new("java").args(...)`.
#[must_use]
pub fn elle_invocation(jar: &str, model: &str, history_path: &str) -> Vec<String> {
    vec![
        "-jar".to_string(),
        jar.to_string(),
        "--model".to_string(),
        model.to_string(),
        history_path.to_string(),
    ]
}

// ─── The checker's identity: version, not just a hash (Design §3/§6) ──────────────────

/// The jar entry carrying elle-cli's own version metadata. A jar is a zip, and Leiningen writes
/// the project's coordinates into this properties file at build time, so it names the version of
/// the code **inside the jar we actually invoke** — not what a filename or a README claims.
///
/// Why not `java -jar <jar> --version`: **verified against the real jar at implementation** —
/// elle-cli 0.1.9 has no version flag at all; `--version` prints `Unknown option: "--version"`
/// **and exits 0**, so keying the report's version on it would silently record an error string as
/// the checker's version. (The same "exit 0 lies" shape as the `:unknown` verdict, Design §3.)
pub const ELLE_VERSION_JAR_ENTRY: &str = "META-INF/maven/elle-cli/elle-cli/pom.properties";

/// The checker's identity, as recorded in the report (Design §6): what it is, which version, and
/// which exact bytes — so an outsider can obtain the same checker and re-run the same verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckerIdentity {
    /// The release version (e.g. `0.1.9`), read from the jar's own metadata.
    pub version: String,
    /// The upstream source revision the jar was built from, when the metadata records one — the
    /// strongest identification available, since it names the checker's source, not just a tag.
    pub revision: Option<String>,
    /// The SHA-256 of the jar file the run actually invoked.
    pub jar_sha256: String,
}

impl CheckerIdentity {
    /// The report's checker field: `elle-cli 0.1.9 (revision <sha>, jar sha256=<sha>)`.
    #[must_use]
    pub fn describe(&self) -> String {
        let revision = match &self.revision {
            Some(r) => format!("revision {r}, "),
            None => String::new(),
        };
        format!(
            "elle-cli {} ({revision}jar sha256={})",
            self.version, self.jar_sha256,
        )
    }
}

/// The `unzip -p <jar> <entry>` argv that extracts elle-cli's version metadata from the jar
/// (Design §3) — pure, so the extraction shape is pinned without a jar on disk.
#[must_use]
pub fn elle_version_extraction(jar: &str) -> Vec<String> {
    vec![
        "-p".to_string(),
        jar.to_string(),
        ELLE_VERSION_JAR_ENTRY.to_string(),
    ]
}

/// Parse elle-cli's version (and, when present, the upstream revision) out of the jar's
/// `pom.properties` text (Design §3) — a `key=value` properties file whose `#` lines are comments.
///
/// A missing or empty `version` key is a hard error, never an `"unknown"` placeholder: the report
/// is the credibility artifact, and a report naming an unidentifiable checker is not one.
///
/// # Errors
/// The properties text carries no `version` key.
pub fn parse_elle_version(properties: &str) -> Result<(String, Option<String>), String> {
    let value_of = |key: &str| -> Option<String> {
        properties
            .lines()
            .map(str::trim)
            .filter(|l| !l.starts_with('#'))
            .find_map(|l| {
                let (k, v) = l.split_once('=')?;
                (k.trim() == key).then(|| v.trim().to_string())
            })
            .filter(|v| !v.is_empty())
    };
    let version = value_of("version").ok_or_else(|| {
        format!(
            "the elle-cli jar's `{ELLE_VERSION_JAR_ENTRY}` carries no `version` key — the \
             checker cannot be identified, and a run report naming an unidentifiable checker is \
             not a credibility artifact"
        )
    })?;
    Ok((version, value_of("revision")))
}

/// The verdict a checker invocation resolved to, per the pinned three-valued contract (Design §3,
/// verified against elle-cli 0.1.9). There is deliberately **no** "unknown, treat as pass" state:
/// only the literal `true` is a [`Pass`], `false` is a genuine [`Violation`], and `:unknown` (or
/// anything unparsable) is [`Inconclusive`] — never a silent pass.
///
/// [`Pass`]: CheckOutcome::Pass
/// [`Violation`]: CheckOutcome::Violation
/// [`Inconclusive`]: CheckOutcome::Inconclusive
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    /// The checker's trailing token was `true` AND the process exited successfully.
    Pass,
    /// The checker's trailing token was `false` — a genuine consistency violation (run FAILURE).
    Violation(String),
    /// The checker returned `:unknown`, an unparsable line, or a `true` token contradicted by a
    /// non-zero exit — the run learned nothing definitive, so it is INCONCLUSIVE (never a pass).
    Inconclusive(String),
}

impl CheckOutcome {
    /// `true` iff this is [`CheckOutcome::Pass`] — the ONLY state the fixtures self-check and
    /// the report may render as a pass.
    #[must_use]
    pub fn is_pass(&self) -> bool {
        matches!(self, CheckOutcome::Pass)
    }

    /// `true` iff this is a genuine [`CheckOutcome::Violation`] (`false` token) — distinct from
    /// merely "not a pass", which an [`CheckOutcome::Inconclusive`] also satisfies.
    #[must_use]
    pub fn is_violation(&self) -> bool {
        matches!(self, CheckOutcome::Violation(_))
    }
}

/// Parse one elle-cli invocation's stdout + exit status into a [`CheckOutcome`], per the pinned
/// contract (Design §3): the per-file output line is `<file>\t<token>`, so the verdict is the
/// **last whitespace-delimited token** of the trailing non-blank line — keyed on the TOKEN, never
/// the exit code alone (verified: `:unknown` exits 0). `true`+success ⇒ [`CheckOutcome::Pass`];
/// `false` ⇒ [`CheckOutcome::Violation`] (regardless of exit); `:unknown`, an unparsable line, or
/// a `true` token contradicted by a non-zero exit ⇒ [`CheckOutcome::Inconclusive`]. Never a
/// silent pass.
#[must_use]
pub fn parse_checker_output(stdout: &str, exit_success: bool) -> CheckOutcome {
    let trailing = stdout.lines().map(str::trim).rev().find(|l| !l.is_empty());
    let Some(line) = trailing else {
        return CheckOutcome::Inconclusive(
            "unparsable checker output — no trailing verdict line; never a silent pass".into(),
        );
    };
    let token = line.split_whitespace().next_back().unwrap_or("");
    match token {
        "true" if exit_success => CheckOutcome::Pass,
        "true" => CheckOutcome::Inconclusive(format!(
            "the checker's trailing token was `true` but the process exited non-zero — the token \
             and the exit status disagree, so the verdict is inconclusive, never a pass: `{line}`"
        )),
        "false" => CheckOutcome::Violation(format!(
            "the checker's trailing token was `false` — a genuine consistency violation: `{line}`"
        )),
        ":unknown" => CheckOutcome::Inconclusive(format!(
            "the checker returned `:unknown` (it could not decide — often a rejected vocabulary) — \
             inconclusive, never a pass: `{line}`"
        )),
        other => CheckOutcome::Inconclusive(format!(
            "unparsable checker output — the trailing token was `{other}`, neither `true`, \
             `false`, nor `:unknown`; never a silent pass: `{line}`"
        )),
    }
}

// ─── Fixtures self-check (Design §5) ───────────────────────────────────────────────────

/// What a well-behaved elle-cli MUST say about a committed golden fixture (Design §5). Each
/// variant is a distinct claim, and the distinctions are the point: "not a pass" is not a caught
/// violation, and "not a violation" is not a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfCheckExpectation {
    /// The known-good fixture: a definite `true`.
    Pass,
    /// The known-bad fixture: a genuine `false` — the checker must actually CATCH the planted
    /// violation, not merely decline to bless the history.
    Violation,
    /// The indeterminate-composed-read fixture: `:unknown`. This one pins the **degrade path's**
    /// premise (Design §2) — when the post-heal sweep cannot resolve a member, the scenario emits
    /// an `:info` composed read, and that is only safe if the checker refuses to give a verdict for
    /// it. A checker build that instead read a read-less/`:info` set history as a **vacuous pass**
    /// would silently turn every degraded run into a green one, so the run confirms it is not
    /// dealing with such a build before it trusts any verdict.
    Inconclusive,
}

/// Whether a fixture's checker-output parse matches what a well-behaved elle-cli SHOULD say about
/// it ([`SelfCheckExpectation`]). Pure comparison over [`parse_checker_output`]'s own result, so it
/// is unit-tested at Check against committed fixture outputs; the off-Check runner reuses this SAME
/// function against the REAL elle-cli's output for the fixtures self-check (both models, both
/// polarities, plus the degrade path — Design §5).
#[must_use]
pub fn self_check_matches(expected: SelfCheckExpectation, actual: &CheckOutcome) -> bool {
    match expected {
        SelfCheckExpectation::Pass => actual.is_pass(),
        SelfCheckExpectation::Violation => actual.is_violation(),
        SelfCheckExpectation::Inconclusive => matches!(actual, CheckOutcome::Inconclusive(_)),
    }
}

// ─── Golden EDN vocabulary (Design §3/§5) ──────────────────────────────────────────────

/// The Elle `rw-register` transaction-history vocabulary
/// [`consistency_workload::MultiProcessHistory::to_elle_edn`] emits (verified against elle-cli
/// 0.1.9) — pinned here as plain tokens (not by importing `wyrd-server`, which the test-graph
/// constraint forbids) so a committed golden EDN fixture can be checked against the vocabulary the
/// checker actually accepts: `:f :txn` with a `[[:w ...]]`/`[[:r ...]]` micro-op `:value`.
pub const EDN_HISTORY_VOCABULARY: [&str; 6] =
    [":process", ":type", ":f", ":txn", ":value", ":time"];

/// Whether `edn` is a bracketed EDN vector (`[...]`) carrying every token in
/// [`EDN_HISTORY_VOCABULARY`] plus a `[[:w` or `[[:r` micro-op — the golden-file vocabulary pin.
/// Does not attempt a full EDN parse (that is the checker's job); it pins the SHAPE a regression
/// to the serializer would break (e.g. reverting to the #406 scalar `:value` the checker rejects).
#[must_use]
pub fn edn_history_has_expected_vocabulary(edn: &str) -> bool {
    let trimmed = edn.trim();
    trimmed.starts_with('[')
        && trimmed.ends_with(']')
        && EDN_HISTORY_VOCABULARY.iter().all(|tok| edn.contains(tok))
        && (edn.contains("[[:w ") || edn.contains("[[:r "))
}

// ─── Opted-in-but-missing-environment error paths (External dependencies) ─────────────

/// What an opted-in checked run needs on the host before it may claim to have run (Design §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Environment {
    /// `docker` — the `deploy/fdb-multi-replica` cluster and the #407 legs' privileges.
    pub docker: bool,
    /// `java` — the JVM elle-cli runs on.
    pub java: bool,
    /// `$WYRD_ELLE_CLI_JAR` names an existing file.
    pub jar_present: bool,
    /// `unzip` — reads the checker's version out of the jar ([`ELLE_VERSION_JAR_ENTRY`]). Required
    /// rather than optional: without it the run can still produce a verdict, but not a report that
    /// *names the checker that produced it* (Design §6), and a run whose report cannot identify
    /// its checker is not the credibility artifact this issue exists to deliver.
    pub unzip: bool,
}

/// The `run_fdb_metadata_tier1`-shaped hard-error rule (`xtask/src/fdb_faults.rs:286`): opted in
/// (`WYRD_TIER1=1`) but missing any part of the run environment is a **hard error**, never a
/// silent skip — a checked run that quietly did not run would be recorded as a verdict nobody
/// earned. Not opted in ⇒ always `Ok` (deferred, not an error).
///
/// # Errors
/// Names every missing piece of the opted-in environment.
pub fn preflight(opted_in: bool, env: Environment) -> Result<(), String> {
    if !opted_in {
        return Ok(());
    }
    let mut missing = Vec::new();
    if !env.docker {
        missing.push("docker");
    }
    if !env.java {
        missing.push("java");
    }
    if !env.jar_present {
        missing.push("the elle-cli standalone jar ($WYRD_ELLE_CLI_JAR)");
    }
    if !env.unzip {
        missing.push("unzip (reads the checker version out of the jar, for the report)");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "WYRD_TIER1=1 but missing: {} — the checked consistency run cannot obtain a real \
             Elle verdict, and skipping it silently would report a verdict that was never \
             tested",
            missing.join(", "),
        ))
    }
}

// ─── The report renderer (Design §6) ───────────────────────────────────────────────────

/// The report's fields (Design §6): the five the issue names — workload, nemesis, history size,
/// model, verdict — plus the two that make the artifact *checkable by an outsider*: which checker
/// build produced the verdict, and what the checked history's integer elements refer to. Kept as
/// owned `String`s so the runner can build them from live run data OR from a witnessed-run
/// write-up, alike.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportInputs {
    /// What was driven (workload parameters).
    pub workload: String,
    /// The nemesis leg + its **typed materialization evidence** (which fault, on which target,
    /// how it provably bit — Design §6, T2).
    pub nemesis: String,
    /// The per-model history size (ops).
    pub history_size: String,
    /// The model(s) checked.
    pub model: String,
    /// The checker's identity — name, **version**, and jar SHA-256 ([`CheckerIdentity::describe`])
    /// — plus the fixtures self-check result. A verdict is only reproducible against a *named*
    /// checker build, so the version is not decoration: a hash alone identifies bytes nobody can
    /// look up, and the report exists to be re-run by someone who is not us.
    pub checker: String,
    /// The directory member name↔id map ([`ReportInputs::member_id_map_field`]) — without it the
    /// `set` history's integer elements are unresolvable, and the report cannot be tied back to
    /// the objects the run actually created.
    pub member_id_map: String,
    /// The verdict.
    pub verdict: String,
}

impl ReportInputs {
    /// Render the nemesis field of the report from the summary's [`NemesisEvidence`] — the fault
    /// class, the target it hit, whether it materialized, and the leg's own diagnosis of HOW it
    /// bit (Design §6, T2). This is the fidelity delta the v1 iteration lacked: the report carries
    /// the leg's sampled observations, not a bare `materialized: true`.
    #[must_use]
    pub fn nemesis_field(evidence: &NemesisEvidence) -> String {
        format!(
            "`{}` on {} — materialized: {} — evidence: {}",
            evidence.kind, evidence.target, evidence.materialized, evidence.diagnosis,
        )
    }

    /// Render the member-id map field (Design §6): every `id -> member` pair the run created, so a
    /// reader can resolve any integer element in the committed `set` history back to the object
    /// the run created on the wire. An empty map is rendered as such rather than omitted — a
    /// directory history whose elements resolve to nothing is a fact about the run worth showing,
    /// not a field to hide.
    #[must_use]
    pub fn member_id_map_field(map: &[MemberId]) -> String {
        if map.is_empty() {
            return "(empty — the run created no directory members)".to_string();
        }
        let pairs: Vec<String> = map
            .iter()
            .map(|m| format!("{} -> `{}`", m.id, m.member))
            .collect();
        format!("{} members: {}", map.len(), pairs.join(", "))
    }

    /// Render what the composed post-heal full-set `:read` actually observed (Design §2/§6).
    ///
    /// A degraded (`:info`) composed read is reported **in the artifact**, naming the members that
    /// stayed unresolved — not left to be inferred from a bare `:unknown` directory verdict. The
    /// report is the credibility artifact: "the checker declined to judge the directory, and here
    /// is exactly why" is a fact about the run an outsider is entitled to read, and hiding it would
    /// be the same overstatement in reverse.
    #[must_use]
    pub fn composed_final_read_field(summary: &RunSummary) -> String {
        if summary.composed_final_read_determinate {
            return format!(
                "Composed post-heal full-set read: DETERMINATE — observed {} of {} created \
                 elements present {:?}",
                summary.composed_final_read.len(),
                summary.member_id_map.len(),
                summary.composed_final_read,
            );
        }
        format!(
            "Composed post-heal full-set read: INDETERMINATE — {} element(s) {:?} could not be \
             resolved by the sweep even after re-probing, so the read was emitted as `:info` and \
             the `set` model has no definite final read to judge (omitting them from a definite \
             `:ok` read would have claimed they are ABSENT — a fabricated lost element)",
            summary.composed_final_read_unresolved.len(),
            summary.composed_final_read_unresolved,
        )
    }
}

/// Render the Markdown report body (Design §6): the five fields the issue names — workload,
/// nemesis, history size, model, verdict — plus the checker's identity and the member-id map.
/// Pure — the runner writes this under `target/consistency-run/report.md` (every run) and, for the
/// first witnessed run, the SAME text is committed under `docs/design/reviews/` (precedent:
/// `m4-fdb-go-no-go.md`).
#[must_use]
pub fn render_report(inputs: &ReportInputs) -> String {
    format!(
        "# Checked consistency run report (#408)\n\n\
         - **Workload:** {}\n\
         - **Nemesis:** {}\n\
         - **History size:** {}\n\
         - **Model:** {}\n\
         - **Checker:** {}\n\
         - **Member-id map:** {}\n\
         - **Verdict:** {}\n",
        inputs.workload,
        inputs.nemesis,
        inputs.history_size,
        inputs.model,
        inputs.checker,
        inputs.member_id_map,
        inputs.verdict,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attesting_evidence() -> NemesisEvidence {
        NemesisEvidence {
            kind: "partition".into(),
            target: "fdb0 (wyrd-consistency-run-fdb0-1 @ 172.30.58.11:4500)".into(),
            materialized: true,
            diagnosis: "peers_saw_target before=true during=false (must flip true→false), \
                        target_running_during=true"
                .into(),
        }
    }

    fn attesting_summary() -> RunSummary {
        RunSummary {
            workload: "w".into(),
            nemesis: attesting_evidence(),
            genuinely_concurrent: true,
            register_ops: 1,
            directory_ops: 1,
            register_outcomes: OutcomeCounts::default(),
            directory_outcomes: OutcomeCounts::default(),
            delete_pool: DeletePoolSummary {
                ops: 1,
                outcomes: OutcomeCounts::default(),
                checks: DeletePoolChecks {
                    session_read_your_writes: true,
                    session_monotonic_reads: true,
                    reads_monotone_per_key: true,
                },
            },
            member_id_map: vec![MemberId {
                member: "dir/member-1".into(),
                id: 1,
            }],
            composed_final_read: vec![1],
            composed_final_read_determinate: true,
            composed_final_read_unresolved: Vec::new(),
        }
    }

    #[test]
    fn run_plan_visits_the_nemesis_window_between_workload_and_heal() {
        let plan = run_plan();
        let workload = plan.iter().position(|s| *s == "workload").unwrap();
        let window = plan.iter().position(|s| *s == "nemesis-window").unwrap();
        let heal = plan.iter().position(|s| *s == "heal").unwrap();
        assert!(workload < window && window < heal);
    }

    #[test]
    fn default_leg_is_partition() {
        assert_eq!(
            selected_leg(None).unwrap(),
            NemesisLegKind::NetworkPartition
        );
        assert_eq!(
            selected_leg(Some("partition")).unwrap(),
            NemesisLegKind::NetworkPartition
        );
    }

    #[test]
    fn evaluate_summary_requires_both_attestations() {
        assert!(evaluate_summary(&attesting_summary()).is_ok());

        let mut vacuous = attesting_summary();
        vacuous.genuinely_concurrent = false;
        assert_eq!(
            evaluate_summary(&vacuous),
            Err(InconclusiveReason::NotGenuinelyConcurrent)
        );

        let mut phantom = attesting_summary();
        phantom.nemesis.materialized = false;
        assert_eq!(
            evaluate_summary(&phantom),
            Err(InconclusiveReason::NemesisNotMaterialized)
        );

        // A sweep that could not resolve every member composed no definite set: the `set` model has
        // no final read to judge, so the run is inconclusive rather than reported.
        let mut unresolved = attesting_summary();
        unresolved.composed_final_read_determinate = false;
        unresolved.composed_final_read_unresolved = vec![7];
        assert_eq!(
            evaluate_summary(&unresolved),
            Err(InconclusiveReason::FinalReadIndeterminate)
        );
    }

    #[test]
    fn checker_output_is_three_valued_and_never_silently_passes() {
        assert!(parse_checker_output("h.edn \t true", true).is_pass());
        assert!(parse_checker_output("h.edn \t false", false).is_violation());
        // `:unknown` exits 0 — must still be inconclusive, keyed on the token not the exit.
        assert!(matches!(
            parse_checker_output("h.edn \t :unknown", true),
            CheckOutcome::Inconclusive(_)
        ));
        assert!(!parse_checker_output("h.edn \t :unknown", true).is_pass());
        // A `true` token contradicted by a non-zero exit is inconclusive, never a pass.
        assert!(!parse_checker_output("h.edn \t true", false).is_pass());
        assert!(!parse_checker_output("garbage", true).is_pass());
        assert!(!parse_checker_output("", true).is_pass());
    }

    #[test]
    fn the_nemesis_field_carries_the_typed_evidence_not_a_bare_boolean() {
        let field = ReportInputs::nemesis_field(&attesting_evidence());
        assert!(field.contains("partition"));
        assert!(field.contains("fdb0"));
        assert!(field.contains("during=false"));
    }
}
