//! The **composable nemesis** seam (#407, slice 4 of #329; ADR-0041 "nemesis first, then the
//! checked artifact") — three fault classes over the real multi-node M4 metadata cluster, each
//! behind ONE importable lifecycle seam usable by both the Tier-1 battery (`#442`) and #408's
//! server-side checked workload.
//!
//! # Why here, and not on [`ClusterFault`](crate::ClusterFault)
//!
//! [`ClusterFault`](crate::ClusterFault) is **partition-shaped by contract** (`topology()`,
//! peer-liveness, heal-rule completeness) and is the #442 battery's seam — it is deliberately
//! NOT generalized (Design §1). The nemesis carries a *different* abstraction: a leg lifecycle
//! (`plan → apply → confirm-materialized → heal → confirm-healed`) with **typed materialization
//! evidence per leg**, so a partition, a clock-skew and a process-pause can all be driven by the
//! same runner while each proves it bit in its OWN terms.
//!
//! # The #442 rule, per leg: a fault that did not bite is INCONCLUSIVE, never a silent pass
//!
//! Every leg carries a [`MaterializationEvidence`] whose `materialized()` is **pure decision
//! logic over sampled observations** — the same code the live impls build from `docker` /
//! `fdbcli status json` output AND the code the Check-time `nemesis_oracles` test exercises
//! red→green. [`drive_leg`] refuses to run the workload (fails as inconclusive) when the
//! evidence says the fault never materialized — a cut the cluster never noticed proves nothing.
//!
//! # The lifecycle ENCLOSES the workload, and never leaks fault state
//!
//! [`drive_leg`] runs the caller's `workload` **while the fault is still active** — between
//! `confirm_materialized` (the pre-workload materialization gate) and `heal` — so the workload
//! genuinely runs under the fault. It heals on *every* exit path: an `apply`/`confirm` failure
//! (a partially-applied `iptables` cut, a half-done recreate), a non-materialized bail, AND a
//! **panicking** workload (the checked #408 workload panics by design on a violation) — the last
//! via `catch_unwind`, so the fault is torn down before the panic resumes. This mirrors
//! `MasterIsolation`'s `Drop` guard (`tier1_metadata_consistency.rs:336-349`): no leg may leave a
//! cut cluster, a paused container, or a skewed clock behind (Invariant B).
//!
//! # What is default-compiled vs deferred
//!
//! Everything in this module is an ordinary-`std` lib with **no `libfdb_c` linkage** (the leg
//! impls are `std::process::Command` shell-outs to `docker` / `fdbcli`), so it compiles
//! unconditionally and is importable by the battery's tests and by #408's
//! `crates/server/tests/` scenario. The *live* three-leg runs against `deploy/fdb-multi-replica`
//! are opt-in (`WYRD_TIER1=1`) and off-Check; the pure decision logic they turn on is unit-tested
//! at Check.

use std::cell::{Cell, RefCell};
use std::panic::AssertUnwindSafe;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use wyrd_testkit::{fdb_peer_sees_target_live, heal_is_complete, partition_took_effect};

/// Which of the three fault classes a nemesis leg injects. Both #408 and the battery import
/// this to enumerate the campaign.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NemesisLegKind {
    /// Live-node **network partition** — an in-netns `iptables` DROP (the #399 technique) that
    /// keeps the target container `running` while its peers lose it.
    Partition,
    /// **Clock skew** — a static per-leg offset applied to ONE cluster node via
    /// container-scoped `libfaketime` (`LD_PRELOAD` + `FAKETIME`), the harness/client clock
    /// left untouched (Design §3).
    ClockSkew,
    /// **Process pause** — a freezer-cgroup `docker pause`/`unpause`: the target serves before,
    /// serves nothing while `paused`, and serves again after.
    ProcessPause,
}

impl NemesisLegKind {
    /// The three legs of the metadata nemesis campaign, in order. A leg dropped from this list
    /// is a fault class the nemesis silently stopped exercising — the `nemesis_oracles` test
    /// pins the set so that regression flips red, not merely absent (the born-at-tier bar).
    pub const ALL: [NemesisLegKind; 3] = [
        NemesisLegKind::Partition,
        NemesisLegKind::ClockSkew,
        NemesisLegKind::ProcessPause,
    ];

    /// A stable, human-readable slug — the fault-class name in diagnostics and (mirrored,
    /// independently) in the xtask dispatch scenario-function names.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            NemesisLegKind::Partition => "partition",
            NemesisLegKind::ClockSkew => "clock-skew",
            NemesisLegKind::ProcessPause => "process-pause",
        }
    }
}

/// Typed evidence that a leg's fault **provably bit**, sampled while the fault is still active
/// (the pre-workload gate). `materialized()` is the per-leg oracle: pure decision logic over
/// recorded observations, so it is unit-checkable at Check and is the very code the live impl
/// calls (a regression flips both). The *recovery* transition (the target serving again) is a
/// separate heal-completeness check ([`heal_is_complete`]), not part of this gate — so the
/// workload can run WHILE the fault is active without unpausing/healing to prove materialization.
pub trait MaterializationEvidence: std::fmt::Debug {
    /// The leg this evidence belongs to.
    fn kind(&self) -> NemesisLegKind;
    /// Whether the fault materialized. `false` ⇒ the run is **inconclusive** and MUST fail —
    /// never pass silently (the #442 "a note is not a gate" rule).
    fn materialized(&self) -> bool;
    /// A human-readable reason a non-materialized run was rejected.
    fn diagnosis(&self) -> String;
}

/// **Partition leg oracle** (Design §2): the survivors' reachability of the target **flips**
/// (`partition_took_effect`: seen before, unseen during) WHILE the target container stays
/// `running`. The running-state clause is what distinguishes a partition from a crash — a
/// container that died is not the fault this leg claims to inject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionEvidence {
    /// A survivor saw the target live BEFORE the cut (the pre-fault sample; must be `true` or a
    /// broken oracle that always says "not live" could manufacture a fault).
    pub peers_saw_target_before: bool,
    /// A survivor still saw the target live DURING the cut. `true` after the whole window ⇒ a
    /// no-op cut, and the leg is NOT materialized.
    pub peers_saw_target_during: bool,
    /// The target container was `running` throughout the window (a partition, not a crash).
    pub target_running_during: bool,
}

impl PartitionEvidence {
    /// The pure oracle: reachability flipped AND the target stayed up.
    #[must_use]
    pub fn materialized(&self) -> bool {
        partition_took_effect(self.peers_saw_target_before, self.peers_saw_target_during)
            && self.target_running_during
    }
}

impl MaterializationEvidence for PartitionEvidence {
    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::Partition
    }
    fn materialized(&self) -> bool {
        PartitionEvidence::materialized(self)
    }
    fn diagnosis(&self) -> String {
        format!(
            "peers_saw_target before={} during={} (must flip true→false), target_running_during={} \
             (must be true — a crash is not a partition)",
            self.peers_saw_target_before, self.peers_saw_target_during, self.target_running_during,
        )
    }
}

/// **Process-pause leg oracle** (Design §2): the target served BEFORE the freeze and served
/// NOTHING during the window WHILE `docker inspect` reported it `paused` (the freezer-cgroup
/// state itself, distinguishing a pause from a crash). Never a single probe: a lone "not
/// serving" sample cannot tell a pause from a partition or a crash — the `paused` inspect state
/// is required.
///
/// The **third** transition (serving again after unpause) is proven by the leg's `heal` +
/// [`NemesisLeg::confirm_healed`] and enforced by [`heal_is_complete`] in [`drive_leg`], NOT by
/// this gate — so the workload runs WHILE the container is still paused (the pause encloses the
/// workload) rather than after an early unpause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PauseEvidence {
    /// The target was observed serving before the freeze.
    pub served_before: bool,
    /// The target was observed serving during the freeze — MUST be `false` (sampled by a
    /// settle-window poll, never a single immediate probe).
    pub served_during: bool,
    /// `docker inspect` reported the container `paused` during the window — the freezer-cgroup
    /// state itself, not merely an absence of service (which a crash would also produce).
    pub inspected_paused_during: bool,
}

impl PauseEvidence {
    /// The pure oracle: served before, served nothing during, and the container was `paused`.
    #[must_use]
    pub fn materialized(&self) -> bool {
        self.served_before && !self.served_during && self.inspected_paused_during
    }
}

impl MaterializationEvidence for PauseEvidence {
    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::ProcessPause
    }
    fn materialized(&self) -> bool {
        PauseEvidence::materialized(self)
    }
    fn diagnosis(&self) -> String {
        format!(
            "served before={} during={} (must be true/false), inspected_paused_during={} \
             (must be true — absence of service without a `paused` state could be a crash)",
            self.served_before, self.served_during, self.inspected_paused_during,
        )
    }
}

/// **Clock-skew leg oracle** (Design §2/§3): an in-container probe **sharing the target
/// container's exact preload/env** shows the target's clock offset from the harness clock by at
/// least the configured floor. A zero floor never materializes — no skew was asked for, so no
/// skew is evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkewEvidence {
    /// Observed signed offset (target clock − harness clock), in seconds — see
    /// [`clock_offset_secs`]. The magnitude is what is compared to the floor; the leg may skew
    /// forward or back.
    pub observed_offset_secs: i64,
    /// The configured floor magnitude, in seconds. A leg configured with `0` cannot materialize.
    pub floor_secs: u64,
}

impl SkewEvidence {
    /// The pure oracle: the offset magnitude clears a non-zero floor.
    #[must_use]
    pub fn materialized(&self) -> bool {
        self.floor_secs > 0 && self.observed_offset_secs.unsigned_abs() >= self.floor_secs
    }
}

impl MaterializationEvidence for SkewEvidence {
    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::ClockSkew
    }
    fn materialized(&self) -> bool {
        SkewEvidence::materialized(self)
    }
    fn diagnosis(&self) -> String {
        format!(
            "|observed_offset|={}s must be ≥ floor={}s and floor must be non-zero",
            self.observed_offset_secs.unsigned_abs(),
            self.floor_secs,
        )
    }
}

/// The nemesis leg lifecycle — the ONE importable seam (Design §1). A leg is `plan`ned, its
/// fault `apply`ed, `confirm_materialized` builds the typed evidence, then it `heal`s and
/// confirms it healed. [`drive_leg`] wraps a caller's workload around this and enforces the two
/// #442 gates (materialized-or-inconclusive, complete-heal). #408 consumes this WITHOUT
/// reopening the lifecycle logic.
///
/// **Contract:** `apply` injects the fault (so a failed `apply`/`confirm` MUST be followed by
/// `heal` — [`drive_leg`] does this). `confirm_materialized` samples the world WHILE the fault is
/// active and MUST NOT remove it (no early unpause / no early recreate-without-override), because
/// the workload runs after it, still under the fault. The recovery observation belongs to
/// [`confirm_healed`].
pub trait NemesisLeg {
    /// The typed materialization evidence this leg produces.
    type Evidence: MaterializationEvidence;

    /// Which fault class this leg injects.
    fn kind(&self) -> NemesisLegKind;

    /// Block until the target is ready to take the workload (cluster up and serving). This
    /// injects **no** fault — a `plan` failure must leave the cluster clean (Invariant B), so a
    /// restart-shaped fault (the skew leg's recreate) is applied in [`apply`](NemesisLeg::apply),
    /// never here.
    fn plan(&self) -> Result<(), String>;

    /// Inject the fault. Samples any pre-fault "before" observation the oracle needs first. For
    /// the skew leg this is the recreate-with-override AND the wait for the cluster to
    /// re-stabilize, so the measured workload never races the restart the recreate itself is
    /// (Design §3).
    fn apply(&self) -> Result<(), String>;

    /// Sample the world and build the typed materialization evidence — WHILE the fault is still
    /// active. MUST NOT remove the fault.
    fn confirm_materialized(&self) -> Result<Self::Evidence, String>;

    /// Remove the fault, returning the identifiers of what was actually undone (for
    /// [`heal_is_complete`] — a partial heal must not read as healed). Idempotent: [`drive_leg`]
    /// may call it after a failed `apply`, so a heal with nothing applied is a no-op `Ok`.
    fn heal(&self) -> Result<Vec<String>, String>;

    /// The fault identifiers that were applied, for the heal-completeness check.
    fn applied_rules(&self) -> Vec<String>;

    /// Poll for up to `timeout` until the target is confirmed serving again — the recovery side
    /// of the #442 heal gate AND (for the pause leg) the third serve→pause→**serve** transition.
    fn confirm_healed(&self, timeout: Duration) -> bool;
}

/// Drive one nemesis leg end-to-end around a caller-supplied `workload`, enforcing the two #442
/// gates: a fault that did not bite FAILS as **inconclusive** (never runs the workload under a
/// phantom fault), and an incomplete heal FAILS (no leaked fault state). This is the runner both
/// the battery and #408 call; the workload is whatever the caller measures under the fault, and
/// it runs WHILE the fault is active (the fault encloses the workload).
///
/// Heals on every exit path — a failed `apply`/`confirm`, a non-materialized bail, and a
/// **panicking** workload (propagated after the heal via `resume_unwind`) — so no leg ever leaks
/// a cut, a paused container, or a skewed clock (Invariant B; mirrors `MasterIsolation`'s `Drop`
/// guard).
///
/// # Errors
/// Returns the leg's own error on plan/apply/heal failure, an inconclusive error when the
/// evidence says the fault never materialized, or a heal-incomplete error.
///
/// # Panics
/// Re-raises a panic thrown by `workload` — AFTER healing the fault. If the heal ALSO failed or was
/// incomplete, it re-raises a panic naming the **leaked fault** instead (a leaked cut/pause/skew is
/// the graver failure and must never hide behind the workload's own panic).
pub fn drive_leg<L, W, T>(leg: &L, workload: W) -> Result<T, String>
where
    L: NemesisLeg,
    W: FnOnce() -> T,
{
    leg.plan()?;

    // From here a fault may be (partially) applied; every early return must heal so a partial
    // `iptables` cut or a half-done recreate never leaks (mirrors MasterIsolation's Drop guard). And
    // the heal itself must be VERIFIED on these early paths — not `let _ = leg.heal()` — or a heal
    // that fails/partial (e.g. `iptables -D` fails at rule 2 of 4) leaks fault state that the
    // primary error never names. `heal_and_report` applies the SAME leak verdict here as the happy
    // and panicking paths use, so no exit path can drop a leaked cut/pause/skew.
    if let Err(e) = leg.apply() {
        return Err(heal_and_report(
            leg,
            format!("nemesis `{}` leg apply failed: {e}", leg.kind().as_str()),
        ));
    }

    let evidence = match leg.confirm_materialized() {
        Ok(ev) => ev,
        Err(e) => {
            return Err(heal_and_report(
                leg,
                format!(
                    "nemesis `{}` leg confirm_materialized failed: {e}",
                    leg.kind().as_str()
                ),
            ));
        }
    };
    if !evidence.materialized() {
        // Heal before bailing so an un-materialized leg never leaks fault state — and surface a
        // failed/incomplete heal alongside the inconclusive verdict, never swallow it.
        return Err(heal_and_report(
            leg,
            format!(
                "nemesis `{}` leg did NOT materialize — inconclusive, refusing to run the workload \
                 under a fault that did not bite (#442): {}",
                leg.kind().as_str(),
                evidence.diagnosis(),
            ),
        ));
    }

    // Run the workload UNDER the still-active fault. Catch a panic so a panicking workload (the
    // checked #408 workload panics by design on a violation) still heals before the unwind
    // resumes — the fault must never outlive the workload.
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(workload));

    // Always heal, whatever the workload did — then evaluate heal completeness BEFORE deciding how
    // to propagate the workload's outcome. A leaked fault (a failed/incomplete heal) is a graver
    // failure than the workload's own panic and MUST NOT be silently dropped underneath a resuming
    // unwind (the prior iteration's defect: `resume_unwind` ran before the heal check).
    let heal_result = leg.heal();
    let live_after = leg.confirm_healed(Duration::from_secs(60));
    let leak = heal_incomplete_reason(leg, &heal_result, live_after);

    match outcome {
        Ok(result) => match leak {
            Some(reason) => Err(reason),
            None => Ok(result),
        },
        Err(panic) => match leak {
            // The workload panicked AND the fault leaked: surface the leak loudly and escalate to
            // a panic that names it, rather than resuming the original panic and hiding the leaked
            // cut/pause/skew. A leaked fault is the invariant this whole seam exists to forbid.
            Some(reason) => {
                eprintln!("nemesis `{}` leg: {reason}", leg.kind().as_str());
                panic!(
                    "nemesis `{}` leg leaked fault state while the workload panicked — {reason}",
                    leg.kind().as_str(),
                );
            }
            // The workload panicked but the fault healed cleanly: propagate the original panic
            // unchanged.
            None => std::panic::resume_unwind(panic),
        },
    }
}

/// Heal the leg on an EARLY exit path (apply-failed / confirm-failed / un-materialized), then fold
/// any heal leak into `primary` so the caller returns ONE error naming both the primary reason and
/// the leaked fault. This is the early-path counterpart of the happy/panic paths' leak handling —
/// every `drive_leg` exit runs the same [`heal_incomplete_reason`] verdict, so a heal that
/// fails/partial on an early path can never be silently dropped (the iteration-3 defect). #408
/// imports `drive_leg` directly and gets no `compose down -v` backstop, so this is its only guard.
fn heal_and_report<L: NemesisLeg>(leg: &L, primary: String) -> String {
    let heal_result = leg.heal();
    let live_after = leg.confirm_healed(Duration::from_secs(60));
    match heal_incomplete_reason(leg, &heal_result, live_after) {
        Some(leak) => format!("{primary}; ADDITIONALLY the heal leaked fault state — {leak}"),
        None => primary,
    }
}

/// Why a heal did not fully undo the fault, or `None` when it did. `Some` ⇒ the leg leaked fault
/// state (Invariant B): either `heal()` itself errored, or the applied rules were not all removed /
/// the target never recovered ([`heal_is_complete`]). Kept as one helper so [`drive_leg`] applies
/// the SAME leak verdict on both the happy and the panicking exit path.
fn heal_incomplete_reason<L: NemesisLeg>(
    leg: &L,
    heal_result: &Result<Vec<String>, String>,
    live_after: bool,
) -> Option<String> {
    match heal_result {
        Err(e) => Some(format!(
            "nemesis `{}` leg heal failed (fault may be leaked): {e}",
            leg.kind().as_str(),
        )),
        Ok(healed) => (!heal_is_complete(&leg.applied_rules(), healed, live_after)).then(|| {
            format!(
                "nemesis `{}` leg did NOT heal completely (rules leaked or target still down) — \
                 Invariant B forbids leaked fault state",
                leg.kind().as_str(),
            )
        }),
    }
}

// ─── Pure parse helpers the live impls build their evidence from ──────────────────────────
//
// Kept pure (string/int in, bool/int out) so the `nemesis_oracles` test binds the live impls'
// decision logic without a container, exactly as `wyrd_testkit`'s partition parsers do.

/// Whether `docker inspect --format '{{.State.Status}}'` output reports the container `paused`.
#[must_use]
pub fn inspected_status_is_paused(status: &str) -> bool {
    status.trim() == "paused"
}

/// Whether `docker inspect --format '{{.State.Status}}'` output reports the container `running`.
#[must_use]
pub fn inspected_status_is_running(status: &str) -> bool {
    status.trim() == "running"
}

/// The signed clock offset `container_epoch − harness_epoch`, in seconds — the magnitude the
/// skew oracle compares to its floor. Positive ⇒ the container clock runs ahead of the harness.
#[must_use]
pub fn clock_offset_secs(container_epoch: i64, harness_epoch: i64) -> i64 {
    container_epoch - harness_epoch
}

/// Whether a survivor's `fdbcli status json` reports the cluster **fully recovered with its data
/// healthy** — the signal the clock-skew leg polls for AFTER each `--force-recreate` before it
/// opens the measured workload window, so the leg measures the skew, never the node restart /
/// re-replication (Design §3 — "the leg measures skew, never the restart").
///
/// The fdb services carry **no `volumes:`** (`deploy/fdb-multi-replica/docker-compose.yml`), so a
/// `--force-recreate` wipes the node's storage; `available` alone would flip `true` while the
/// `double`-redundancy cluster is still re-replicating the wiped node's shards — the exact
/// transient the brief promises the workload window excludes. So this requires ALL THREE of:
/// `client.database_status.available` (the cluster can serve), `cluster.data.state.healthy`
/// (replication is restored — no data movement outstanding), and
/// `cluster.recovery_state.name == "fully_recovered"` (the transaction subsystem finished its
/// recovery). Pure string parse (same style as [`fdb_peer_sees_target_live`]), so it is
/// unit-checkable at Check while the live leg calls this very function.
#[must_use]
pub fn fdb_cluster_fully_recovered(status_json: &str) -> bool {
    let compact: String = status_json.chars().filter(|c| !c.is_whitespace()).collect();
    let available =
        json_bool_after(&compact, "\"database_status\":", "\"available\":") == Some(true);
    let data_healthy = json_bool_after(&compact, "\"data\":", "\"healthy\":") == Some(true);
    let recovered = json_str_after(&compact, "\"recovery_state\":", "\"name\":\"").as_deref()
        == Some("fully_recovered");
    available && data_healthy && recovered
}

/// The first `true`/`false` following `key` after `anchor` in a whitespace-stripped JSON body, or
/// `None`. A scoped sibling of [`fdb_peer_sees_target_live`]'s coordinator parse, kept private —
/// `anchor` bounds the search so a `key` that also appears elsewhere (e.g. `healthy` under both
/// `database_status` and `cluster.data.state`) reads the intended one.
fn json_bool_after(compact: &str, anchor: &str, key: &str) -> Option<bool> {
    let at = compact.find(anchor)?;
    let rest = &compact[at + anchor.len()..];
    let ks = rest.find(key)? + key.len();
    let after = &rest[ks..];
    if after.starts_with("true") {
        Some(true)
    } else if after.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

/// The quoted string value following `key` (which must end at the opening quote) after `anchor`.
fn json_str_after(compact: &str, anchor: &str, key: &str) -> Option<String> {
    let at = compact.find(anchor)?;
    let rest = &compact[at + anchor.len()..];
    let ks = rest.find(key)? + key.len();
    let after = &rest[ks..];
    let end = after.find('"')?;
    Some(after[..end].to_string())
}

// ─── The three live-leg impls (default-compiled, docker shell-outs; off-Check) ─────────────

fn run_docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("spawn `docker {}`: {e}", args.join(" ")))?;
    if !out.status.success() {
        return Err(format!(
            "`docker {}` failed ({}): {}",
            args.join(" "),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn harness_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A survivor's `fdbcli --exec "status json"` body, or `None` when the exec failed. Passes
/// `--timeout 10` exactly as `support::status_json` does — an unbounded `fdbcli` against a
/// half-cut cluster can hang the whole leg.
fn survivor_status_json(survivor_container: &str) -> Option<String> {
    run_docker(&[
        "exec",
        survivor_container,
        "fdbcli",
        "--timeout",
        "10",
        "--exec",
        "status json",
    ])
    .ok()
}

/// `docker inspect --format {{.State.Status}} <container>`, trimmed.
fn container_status(container: &str) -> Option<String> {
    run_docker(&["inspect", "--format", "{{.State.Status}}", container])
        .ok()
        .map(|s| s.trim().to_string())
}

/// **Live-node network-partition leg** — re-implements the `MasterIsolation` technique
/// (`crates/metadata-fdb/tests/tier1_metadata_consistency.rs:232`, test-binary-private, cannot
/// be imported) here in the ordinary lib: an in-netns symmetric `iptables` DROP with a
/// survivor-side `status json` reachability oracle. The target container stays `running`, so
/// the oracle can tell the partition from a crash.
pub struct PartitionLeg {
    /// The isolated process's `<ip>:<port>` address, as `status json` names it.
    pub target_addr: String,
    /// Just the IP, for the `iptables -s/-d` rules.
    pub target_ip: String,
    /// The container owning the target's netns (where the rules are applied) and whose
    /// lifecycle-state the oracle inspects.
    pub target_container: String,
    /// A **surviving** container the oracle runs `fdbcli` in — never the target.
    pub survivor_container: String,
    /// The privileged in-netns `iptables` image (`wyrd-iptables:local`).
    pub iptables_image: String,
    applied: RefCell<Vec<Vec<String>>>,
    removed: RefCell<Vec<String>>,
    saw_before: Cell<bool>,
}

impl PartitionLeg {
    /// Construct a partition leg. `saw_before` is captured at [`apply`](NemesisLeg::apply).
    #[must_use]
    pub fn new(
        target_addr: impl Into<String>,
        target_ip: impl Into<String>,
        target_container: impl Into<String>,
        survivor_container: impl Into<String>,
        iptables_image: impl Into<String>,
    ) -> Self {
        Self {
            target_addr: target_addr.into(),
            target_ip: target_ip.into(),
            target_container: target_container.into(),
            survivor_container: survivor_container.into(),
            iptables_image: iptables_image.into(),
            applied: RefCell::new(Vec::new()),
            removed: RefCell::new(Vec::new()),
            saw_before: Cell::new(false),
        }
    }

    /// The symmetric rule set: DROP on both chains, on both selectors.
    fn rules(&self) -> Vec<Vec<String>> {
        let mut rules = Vec::new();
        for chain in ["INPUT", "OUTPUT"] {
            for sel in ["-s", "-d"] {
                rules.push(vec![
                    chain.to_string(),
                    sel.to_string(),
                    self.target_ip.clone(),
                    "-j".to_string(),
                    "DROP".to_string(),
                ]);
            }
        }
        rules
    }

    fn iptables(&self, args: &[String]) -> Result<(), String> {
        let mut full = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--privileged".to_string(),
            format!("--network=container:{}", self.target_container),
            self.iptables_image.clone(),
        ];
        full.extend(args.iter().cloned());
        let refs: Vec<&str> = full.iter().map(String::as_str).collect();
        run_docker(&refs).map(|_| ())
    }

    fn peers_see_target_live(&self) -> bool {
        match survivor_status_json(&self.survivor_container) {
            Some(status) => fdb_peer_sees_target_live(&status, &self.target_addr),
            None => false,
        }
    }
}

impl NemesisLeg for PartitionLeg {
    type Evidence = PartitionEvidence;

    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::Partition
    }

    fn plan(&self) -> Result<(), String> {
        let deadline = SystemTime::now() + Duration::from_secs(90);
        while SystemTime::now() < deadline {
            if self.peers_see_target_live() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        Err("partition leg: cluster never reported the target live within 90s".into())
    }

    fn apply(&self) -> Result<(), String> {
        // Sample the "before" view BEFORE cutting — the oracle's pre-fault reference.
        self.saw_before.set(self.peers_see_target_live());
        for rule in self.rules() {
            let mut args = vec!["-I".to_string()];
            args.extend(rule.iter().cloned());
            self.iptables(&args)?;
            self.applied.borrow_mut().push(rule);
        }
        Ok(())
    }

    fn confirm_materialized(&self) -> Result<PartitionEvidence, String> {
        // Poll the survivors' view for up to 45s; `during == true` after the whole window is a
        // no-op cut (recorded, not hidden). The container must stay `running` throughout.
        let deadline = SystemTime::now() + Duration::from_secs(45);
        let mut during = true;
        while SystemTime::now() < deadline {
            if !self.peers_see_target_live() {
                during = false;
                break;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        let running = container_status(&self.target_container)
            .map(|s| inspected_status_is_running(&s))
            .unwrap_or(false);
        Ok(PartitionEvidence {
            peers_saw_target_before: self.saw_before.get(),
            peers_saw_target_during: during,
            target_running_during: running,
        })
    }

    fn heal(&self) -> Result<Vec<String>, String> {
        // Idempotent: skip a rule already removed (drive_leg may call heal twice on a failed
        // apply). Only newly-removed rules are returned, but `applied_rules ⊆ removed` overall.
        let mut healed = Vec::new();
        for rule in self.applied.borrow().iter() {
            let id = rule.join(" ");
            if self.removed.borrow().contains(&id) {
                healed.push(id);
                continue;
            }
            let mut args = vec!["-D".to_string()];
            args.extend(rule.iter().cloned());
            self.iptables(&args)?;
            self.removed.borrow_mut().push(id.clone());
            healed.push(id);
        }
        Ok(healed)
    }

    fn applied_rules(&self) -> Vec<String> {
        self.applied.borrow().iter().map(|r| r.join(" ")).collect()
    }

    fn confirm_healed(&self, timeout: Duration) -> bool {
        let deadline = SystemTime::now() + timeout;
        while SystemTime::now() < deadline {
            if self.peers_see_target_live() {
                return true;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        false
    }
}

/// **Process-pause leg** — `docker pause`/`unpause` of one cluster node. The pause **encloses the
/// workload**: `apply` freezes and samples "served before", `confirm_materialized` proves the
/// freeze bit (a settle-window poll shows the survivors lost the target WHILE `docker inspect`
/// reports it `paused`) but does NOT unpause, the workload then runs under the still-frozen node,
/// and `heal`/`confirm_healed` unpause and prove the third serve→pause→**serve** transition.
/// "Serving" is read from a SURVIVOR's `status json` reachability of the target (never the
/// paused node's own view).
pub struct ProcessPauseLeg {
    /// The `<ip>:<port>` address of the paused process, as `status json` names it.
    pub target_addr: String,
    /// The container to pause/unpause and inspect.
    pub target_container: String,
    /// A surviving container the serving oracle queries.
    pub survivor_container: String,
    served_before: Cell<bool>,
    served_during: Cell<bool>,
    paused_during: Cell<bool>,
    paused: Cell<bool>,
}

impl ProcessPauseLeg {
    /// Construct a pause leg.
    #[must_use]
    pub fn new(
        target_addr: impl Into<String>,
        target_container: impl Into<String>,
        survivor_container: impl Into<String>,
    ) -> Self {
        Self {
            target_addr: target_addr.into(),
            target_container: target_container.into(),
            survivor_container: survivor_container.into(),
            served_before: Cell::new(false),
            served_during: Cell::new(false),
            paused_during: Cell::new(false),
            paused: Cell::new(false),
        }
    }

    fn target_served(&self) -> bool {
        match survivor_status_json(&self.survivor_container) {
            Some(status) => fdb_peer_sees_target_live(&status, &self.target_addr),
            None => false,
        }
    }
}

impl NemesisLeg for ProcessPauseLeg {
    type Evidence = PauseEvidence;

    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::ProcessPause
    }

    fn plan(&self) -> Result<(), String> {
        let deadline = SystemTime::now() + Duration::from_secs(90);
        while SystemTime::now() < deadline {
            if self.target_served() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        Err("pause leg: cluster never reported the target serving within 90s".into())
    }

    fn apply(&self) -> Result<(), String> {
        self.served_before.set(self.target_served());
        run_docker(&["pause", &self.target_container])?;
        self.paused.set(true);
        Ok(())
    }

    fn confirm_materialized(&self) -> Result<PauseEvidence, String> {
        // Settle-window poll (mirrors PartitionLeg): the survivors' failure detector takes a few
        // seconds to drop a frozen peer, so a single immediate probe is near-deterministically
        // inconclusive. `during` stays `true` only if the target kept serving the WHOLE window
        // (a no-op freeze). Crucially this does NOT unpause — the workload runs under the freeze.
        let deadline = SystemTime::now() + Duration::from_secs(45);
        let mut during = true;
        while SystemTime::now() < deadline {
            if !self.target_served() {
                during = false;
                break;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        self.served_during.set(during);
        self.paused_during.set(
            container_status(&self.target_container)
                .map(|s| inspected_status_is_paused(&s))
                .unwrap_or(false),
        );
        Ok(PauseEvidence {
            served_before: self.served_before.get(),
            served_during: self.served_during.get(),
            inspected_paused_during: self.paused_during.get(),
        })
    }

    fn heal(&self) -> Result<Vec<String>, String> {
        // Idempotent unpause — the fault was applied in `apply`, never in `confirm_materialized`.
        if self.paused.get() {
            run_docker(&["unpause", &self.target_container])?;
            self.paused.set(false);
        }
        Ok(vec![format!("pause {}", self.target_container)])
    }

    fn applied_rules(&self) -> Vec<String> {
        vec![format!("pause {}", self.target_container)]
    }

    fn confirm_healed(&self, timeout: Duration) -> bool {
        // The third serve→pause→**serve** transition: the survivors see the target serving again.
        let deadline = SystemTime::now() + timeout;
        while SystemTime::now() < deadline {
            if self.target_served() {
                return true;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        false
    }
}

/// **Clock-skew leg** — a static per-leg offset applied to ONE fdbserver container via
/// container-scoped `libfaketime` (`LD_PRELOAD` + `FAKETIME`), recreated through a compose
/// override; heal recreates WITHOUT the override (Design §3). The materialization probe
/// `docker exec`s `date +%s` in the SAME container, so it inherits the same fake clock — the
/// oracle is that process, not an unrelated one. Only cluster-node clocks are skewed, never the
/// harness/client (the #406 history's real-time order stays trustworthy).
///
/// The recreate is a restart-fault, so it runs in [`apply`](NemesisLeg::apply) (never `plan`, or
/// a `plan` failure would leave a permanently skewed node). Because the fdb services carry no
/// `volumes:`, the `--force-recreate` also WIPES the node's storage, so after every recreate the
/// leg polls a **survivor's `status json`** until the cluster is fully recovered and re-replicated
/// ([`fdb_cluster_fully_recovered`], mirroring [`PartitionLeg::plan`]) BEFORE the measured workload
/// window opens — the leg measures skew, never the restart / re-replication (Design §3). `service`,
/// `target_container` and the override's target are supplied by ONE runner resolution, so they
/// cannot disagree (the fdb wiring resolves the container FROM the service).
pub struct ClockSkewLeg {
    /// The compose file that declares the target service (the `deploy/fdb-multi-replica` stack).
    pub compose_file: String,
    /// The compose override that adds the `LD_PRELOAD`/`FAKETIME` env to the target service.
    pub faketime_override: String,
    /// The compose service name of the node to skew (must match the override's target service).
    pub service: String,
    /// The container name to `docker exec` the clock probe in — the SAME node named by `service`.
    pub target_container: String,
    /// A **surviving** container the cluster-recovery oracle runs `fdbcli status json` in — never
    /// the skewed target (which is mid-recreate). Its `status json` reports the whole cluster's
    /// health, so it is the node that answers "has the wiped node re-replicated yet?".
    pub survivor_container: String,
    /// The configured offset floor magnitude, in seconds.
    pub floor_secs: u64,
}

impl ClockSkewLeg {
    /// Construct a clock-skew leg.
    #[must_use]
    pub fn new(
        compose_file: impl Into<String>,
        faketime_override: impl Into<String>,
        service: impl Into<String>,
        target_container: impl Into<String>,
        survivor_container: impl Into<String>,
        floor_secs: u64,
    ) -> Self {
        Self {
            compose_file: compose_file.into(),
            faketime_override: faketime_override.into(),
            service: service.into(),
            target_container: target_container.into(),
            survivor_container: survivor_container.into(),
            floor_secs,
        }
    }

    fn container_epoch(&self) -> Option<i64> {
        run_docker(&["exec", &self.target_container, "date", "+%s"])
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
    }

    fn recreate(&self, with_override: bool) -> Result<(), String> {
        // The compose file carries `name: wyrd-fdb-tier1-metadata`, so the recreate resolves to
        // the SAME project the runner brought up (no `-p` needed).
        let mut args = vec!["compose", "-f", &self.compose_file];
        if with_override {
            args.push("-f");
            args.push(&self.faketime_override);
        }
        args.extend(["up", "-d", "--force-recreate", &self.service]);
        run_docker(&args).map(|_| ())
    }

    fn wait_execable(&self, timeout: Duration) -> Result<i64, String> {
        let deadline = SystemTime::now() + timeout;
        loop {
            if let Some(epoch) = self.container_epoch() {
                return Ok(epoch);
            }
            if SystemTime::now() >= deadline {
                return Err("clock-skew leg: target container never became exec-able".into());
            }
            std::thread::sleep(Duration::from_secs(2));
        }
    }

    /// Poll a survivor's `status json` until the CLUSTER is fully recovered and re-replicated —
    /// NOT merely until the recreated node is `docker exec`-able (which happens ~instantly after
    /// `up -d`, long before the wiped `fdbserver` rejoins and its shards re-replicate). This is the
    /// re-stabilization gate Design §3 requires between a recreate and the measured workload, so
    /// the leg never measures the restart. Mirrors [`PartitionLeg::plan`]'s survivor-side poll.
    fn wait_cluster_recovered(&self, timeout: Duration) -> Result<(), String> {
        let deadline = SystemTime::now() + timeout;
        loop {
            if let Some(status) = survivor_status_json(&self.survivor_container) {
                if fdb_cluster_fully_recovered(&status) {
                    return Ok(());
                }
            }
            if SystemTime::now() >= deadline {
                return Err(
                    "clock-skew leg: cluster did not fully recover / re-replicate after the \
                     force-recreate within the timeout — refusing to open the workload window on a \
                     still-restarting cluster (the leg must measure skew, never the restart)"
                        .into(),
                );
            }
            std::thread::sleep(Duration::from_secs(3));
        }
    }
}

impl NemesisLeg for ClockSkewLeg {
    type Evidence = SkewEvidence;

    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::ClockSkew
    }

    fn plan(&self) -> Result<(), String> {
        // Readiness only — NO fault here (a `plan` failure must leave the node un-skewed). The
        // node must be up and exec-able before we recreate it with the fake clock.
        self.wait_execable(Duration::from_secs(90)).map(|_| ())
    }

    fn apply(&self) -> Result<(), String> {
        // The recreate IS the fault AND a restart, so it happens here. We then wait for the node to
        // be exec-able AND — crucially — for the whole cluster to fully recover and re-replicate the
        // wiped node's shards BEFORE confirm/workload, so the leg measures skew, never the restart
        // (Design §3). A failed recreate is healed by drive_leg (recreate without the override), so
        // a half-applied skew never leaks.
        self.recreate(true)?;
        self.wait_execable(Duration::from_secs(90))?;
        self.wait_cluster_recovered(Duration::from_secs(180))
    }

    fn confirm_materialized(&self) -> Result<SkewEvidence, String> {
        let container_epoch = self
            .container_epoch()
            .ok_or_else(|| "clock-skew leg: could not read the container clock".to_string())?;
        Ok(SkewEvidence {
            observed_offset_secs: clock_offset_secs(container_epoch, harness_epoch_secs()),
            floor_secs: self.floor_secs,
        })
    }

    fn heal(&self) -> Result<Vec<String>, String> {
        // Heal is also a recreate (without the override) and so also wipes + restarts the node; wait
        // for the cluster to fully recover here too, so `drive_leg`'s heal-completeness verdict is
        // taken against a re-stabilized cluster and #408 (which imports drive_leg without the
        // runner's `compose down -v` backstop) never proceeds on a half-recovered cluster.
        self.recreate(false)?;
        self.wait_execable(Duration::from_secs(90))?;
        self.wait_cluster_recovered(Duration::from_secs(180))?;
        Ok(vec![format!("faketime {}", self.service)])
    }

    fn applied_rules(&self) -> Vec<String> {
        vec![format!("faketime {}", self.service)]
    }

    fn confirm_healed(&self, timeout: Duration) -> bool {
        let deadline = SystemTime::now() + timeout;
        while SystemTime::now() < deadline {
            if let Some(epoch) = self.container_epoch() {
                // Healed = the clock is back within the floor of the harness clock.
                if clock_offset_secs(epoch, harness_epoch_secs()).unsigned_abs() < self.floor_secs {
                    return true;
                }
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        false
    }
}
