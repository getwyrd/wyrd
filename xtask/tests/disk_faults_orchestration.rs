//! Unit tests for the Tier-1 disk-fault harness orchestration helpers
//! (`xtask::disk_faults` — the Check-time flippable coverage, born at M3,
//! issue #195, proposal 0005 §13.2, `0005:405-408`).
//!
//! **Why these tests exist.** The Tier-1 disk-fault harness is a "deferred"
//! tier (ADR-0016): its real-device scenario (`crates/custodian/tests/
//! tier1_disk_faults.rs`) requires root + `dmsetup` and is `#[ignore]`d in
//! the unprivileged `cargo xtask ci` gate. "Deferred" means the *green run*
//! is off-Check; it does NOT mean the harness is unbuilt. The deliverable must
//! be **compiled and test-exercised at Check** — that is this file.
//!
//! These tests exercise the **host-independent** orchestration logic:
//!
//! - [`dm_table_linear`] / [`dm_table_error`] — the `dmsetup` table-plan strings
//!   for the scenario phases, pure functions requiring no privileged I/O.
//! - [`verdict_scrub_leg`] / [`verdict_campaign`] / [`verdict_passes`] — the
//!   post-repair assertion helpers.
//!
//! They run inside `cargo xtask ci`'s `cargo test --workspace` and go **RED**
//! when a helper is removed or stubbed — proving the seam is load-bearing, not
//! resting red on non-existence (the "demonstrated red" the brief requires).
//!
//! **C4-verify note.** `run-verify.sh` classifies this file as an added test
//! (`xtask/tests/*.rs` matches `*/tests/*.rs`) and targets it in the RED check:
//! removing `xtask/src/lib.rs` + `xtask/src/disk_faults.rs` makes this file
//! fail to compile (`use xtask::disk_faults::*` has no lib to resolve against),
//! so the RED check observes a non-zero exit and declares RED ✓.

#![forbid(unsafe_code)]

use xtask::disk_faults::{
    dm_table_error, dm_table_linear, verdict_campaign, verdict_passes, verdict_scrub_leg,
};

// ─── dm table-plan helpers ────────────────────────────────────────────────────

#[test]
fn dm_table_linear_produces_correct_dmsetup_string() {
    // The linear target passes I/O straight through — no fault.
    let table = dm_table_linear(32768, "/dev/loop0");
    assert_eq!(
        table, "0 32768 linear /dev/loop0 0",
        "dm_table_linear must produce the exact dmsetup linear table string"
    );
}

#[test]
fn dm_table_linear_embeds_sector_count_and_device() {
    let table = dm_table_linear(65536, "/dev/loop3");
    assert!(
        table.contains("65536"),
        "dm_table_linear must embed the sector count; got: {table:?}"
    );
    assert!(
        table.contains("/dev/loop3"),
        "dm_table_linear must embed the backing device path; got: {table:?}"
    );
    assert!(
        table.starts_with("0 "),
        "table must start at logical sector 0; got: {table:?}"
    );
    assert!(
        table.ends_with(" 0"),
        "table must use device offset 0; got: {table:?}"
    );
}

#[test]
fn dm_table_error_produces_correct_dmsetup_string() {
    // The error target returns EIO for every I/O — the reconstruction-leg fault.
    let table = dm_table_error(32768);
    assert_eq!(
        table, "0 32768 error",
        "dm_table_error must produce the exact dmsetup error table string"
    );
}

#[test]
fn dm_table_error_embeds_sector_count() {
    let table = dm_table_error(131072);
    assert!(
        table.contains("131072"),
        "dm_table_error must embed the sector count; got: {table:?}"
    );
    assert!(
        table.contains(" error"),
        "dm_table_error must name the 'error' target; got: {table:?}"
    );
}

// ─── scrub-leg verdict ────────────────────────────────────────────────────────

#[test]
fn verdict_scrub_leg_detected_when_chunk_enqueued() {
    let v = verdict_scrub_leg(true);
    assert!(
        v.chunk_enqueued,
        "a scrub pass that enqueued the faulted chunk must record chunk_enqueued=true"
    );
}

#[test]
fn verdict_scrub_leg_not_detected_when_queue_empty() {
    // A causally-inert scrub pass (iteration-1 failure mode) must be surfaced as
    // NOT detected — not silently accepted as a pass.
    let v = verdict_scrub_leg(false);
    assert!(
        !v.chunk_enqueued,
        "a scrub pass that enqueued nothing must record chunk_enqueued=false \
         (causally-inert scrub is a verdict FAIL, not a pass)"
    );
}

// ─── campaign verdict ─────────────────────────────────────────────────────────

#[test]
fn campaign_verdict_passes_full_redundancy_no_errors() {
    // The canonical success path: scrub detected, reconstruction ok, full redundancy.
    let v = verdict_campaign(true, true, 3, 3);
    assert!(
        verdict_passes(&v),
        "scrub detected + reconstruction ok + intact=3 == expected=3 → campaign PASS"
    );
}

#[test]
fn campaign_verdict_fails_if_scrub_leg_inert() {
    // A causally-inert scrub leg (the iteration-1 failure mode) must fail the campaign.
    let v = verdict_campaign(false, true, 3, 3);
    assert!(
        !verdict_passes(&v),
        "a causally-inert scrub leg (scrub_detected=false) must fail the campaign verdict"
    );
}

#[test]
fn campaign_verdict_fails_if_reconstruction_propagated_error() {
    // If reconstruction propagated a read error (reconcile_step returned Err),
    // the campaign must fail — is_permanent_read_fault must read AROUND EIO.
    let v = verdict_campaign(true, false, 3, 3);
    assert!(
        !verdict_passes(&v),
        "a propagated read error in reconstruction (reconstruction_ok=false) \
         must fail the campaign verdict"
    );
}

#[test]
fn campaign_verdict_fails_if_chunk_not_at_full_redundancy() {
    // After reconstruction, all n fragments must be intact.
    let v = verdict_campaign(true, true, 2, 3);
    assert!(
        !verdict_passes(&v),
        "partial redundancy (intact_after=2 < expected_frags=3) must fail the campaign"
    );
}

#[test]
fn campaign_verdict_zero_intact_fails() {
    // Degenerate case: zero intact fragments is the worst possible outcome.
    let v = verdict_campaign(true, true, 0, 3);
    assert!(
        !verdict_passes(&v),
        "zero intact fragments must fail the campaign verdict"
    );
}

#[test]
fn campaign_verdict_components_are_recorded_faithfully() {
    // The struct carries the raw inputs so a diagnostic print can show exactly
    // which condition failed, not just "FAIL".
    let v = verdict_campaign(true, false, 2, 3);
    assert!(v.scrub_detected, "scrub_detected must be stored as-is");
    assert!(
        !v.reconstruction_ok,
        "reconstruction_ok must be stored as-is"
    );
    assert_eq!(v.intact_after, 2, "intact_after must be stored as-is");
    assert_eq!(v.expected_frags, 3, "expected_frags must be stored as-is");
}
