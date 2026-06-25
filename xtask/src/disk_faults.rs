//! Tier-1 disk-fault harness orchestration (proposal 0005 §13.2, `0005:405-408`,
//! issue #195): the **host-independent** part of the Tier-1 disk-fault harness —
//! the device-mapper table-plan strings and the post-repair campaign verdict helpers.
//!
//! These are pure functions with no privileged I/O (no `dmsetup`, no loop devices,
//! no filesystem mounts), so they are unit-testable inside the unprivileged
//! `cargo xtask ci` gate and compile on any host (ADR-0016).
//!
//! The privileged scenario that uses the real `dm-error` device lives at
//! `crates/custodian/tests/tier1_disk_faults.rs` and is `#[ignore]`d; `cargo
//! xtask disk-faults` runs it via `cargo test --ignored` in the off-Check Tier-1
//! CI job (`tier1-disk-faults.yml`), opted in by `WYRD_TIER1=1`.
//!
//! # Born-at-tier flippable coverage
//!
//! `xtask/tests/disk_faults_orchestration.rs` imports these helpers and tests them
//! without any root privileges. When a helper is removed or returns a wrong value,
//! those tests go RED inside `cargo xtask ci` — the "deferred ≠ unbuilt" bar (the
//! Verification-posture forcing function from `templates/brief.md.tpl` and INTEGRATION
//! §"Deferred/off-Check tiers").

// ─── Device-mapper table-plan helpers ────────────────────────────────────────

/// Generate the `dmsetup` table string for a **linear passthrough** target.
///
/// The linear target passes all I/O straight through to `device` with no faults —
/// the healthy phase used to set up the dm device and mount the filesystem before
/// the fault injection begins.
///
/// The resulting string is passed verbatim to `dmsetup create --table` /
/// `dmsetup load --table`.
///
/// # Example
///
/// ```
/// let table = xtask::disk_faults::dm_table_linear(32768, "/dev/loop0");
/// assert_eq!(table, "0 32768 linear /dev/loop0 0");
/// ```
#[must_use]
pub fn dm_table_linear(sectors: u64, device: &str) -> String {
    format!("0 {sectors} linear {device} 0")
}

/// Generate the `dmsetup` table string for an **error** target.
///
/// The error target returns `EIO` for every read and write — the block-layer fault
/// the reconstruction leg of the Tier-1 scenario injects after the scrub leg has
/// detected and enqueued the bit-rot repair obligation.
///
/// With `dm-error` active, `FsChunkStore::get_fragment` on the faulted D server
/// receives `EIO` from the OS. `reconstruction::is_permanent_read_fault` (the
/// production fix from issue #251) classifies this as a permanent read fault and
/// reads around it, rebuilding the missing shard from the ≥ `k` survivors.
///
/// # Example
///
/// ```
/// let table = xtask::disk_faults::dm_table_error(32768);
/// assert_eq!(table, "0 32768 error");
/// ```
#[must_use]
pub fn dm_table_error(sectors: u64) -> String {
    format!("0 {sectors} error")
}

// ─── Post-repair verdict helpers ──────────────────────────────────────────────

/// The **scrub-leg verdict**: whether scrub detected the injected bit-rot fault and
/// enqueued a repair obligation for the faulted chunk.
///
/// A scrub pass that enqueues nothing is **causally inert** — the iteration-1
/// failure mode (brief §"Iteration 1 carry-forward"): the fault was load-bearing
/// only for read assertions but did not drive the repair path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubLegVerdict {
    /// Scrub detected the injected fault and enqueued a repair obligation for
    /// the faulted chunk on the shared repair queue.
    pub chunk_enqueued: bool,
}

/// Evaluate the scrub leg: did scrub detect the injected fault and enqueue a
/// repair obligation for the faulted chunk?
///
/// `queue_contains_chunk` — whether the shared repair queue contains the faulted
/// chunk's id immediately after the scrub `reconcile_step`.
#[must_use]
pub fn verdict_scrub_leg(queue_contains_chunk: bool) -> ScrubLegVerdict {
    ScrubLegVerdict {
        chunk_enqueued: queue_contains_chunk,
    }
}

/// The **full Tier-1 campaign verdict**: both the scrub leg AND the reconstruction
/// leg must pass for the campaign to be accepted.
///
/// The two binding acceptance conditions (brief §"Success criterion"):
/// 1. `chunk → full redundancy` — all `n = k + m` fragments are intact after
///    reconstruction (`intact_after == expected_frags`).
/// 2. `read_errors == 0` — no read error was propagated to the caller
///    (`reconcile_step` returned `Ok`, so `reconstruction_ok` is `true`).
///
/// The scrub leg must also have been load-bearing (`scrub_detected` is `true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignVerdict {
    /// Scrub detected the injected fault and enqueued a repair obligation.
    pub scrub_detected: bool,
    /// Reconstruction completed without propagating a read error to the caller
    /// (`reconcile_step` returned `Ok`).
    pub reconstruction_ok: bool,
    /// Number of fragments at full integrity after reconstruction.
    pub intact_after: usize,
    /// Expected fragment count (`n = k + m` for the chunk's `EcScheme`).
    pub expected_frags: usize,
}

/// Evaluate the full Tier-1 campaign verdict from its components.
#[must_use]
pub fn verdict_campaign(
    scrub_detected: bool,
    reconstruction_ok: bool,
    intact_after: usize,
    expected_frags: usize,
) -> CampaignVerdict {
    CampaignVerdict {
        scrub_detected,
        reconstruction_ok,
        intact_after,
        expected_frags,
    }
}

/// Whether the campaign verdict passes the Tier-1 acceptance criteria:
///
/// 1. Scrub was load-bearing (detected the fault and enqueued a repair).
/// 2. Reconstruction completed without a propagated read error.
/// 3. The chunk is at full redundancy (`intact_after == expected_frags`).
#[must_use]
pub fn verdict_passes(v: &CampaignVerdict) -> bool {
    v.scrub_detected && v.reconstruction_ok && v.intact_after == v.expected_frags
}
