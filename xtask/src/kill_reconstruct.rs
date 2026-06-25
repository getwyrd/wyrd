//! Host-independent orchestration helpers for the Tier-2 kill-and-reconstruct
//! harness (proposal 0005 §13.2, `0005:409-411`; the crate touch-point `0005:437-438`).
//!
//! Pure functions extracted from the orchestration flow so they are unit-testable
//! inside `cargo xtask ci` without a container runtime or privileged environment —
//! the "born-at-tier coverage" the verification posture requires (brief §"Verification
//! posture"). The live orchestration that threads these through the compose/finalize
//! plumbing lives in [`crate::faults::run_kill_reconstruct`].
//!
//! The assertion helpers (`assert_garbage_not_corruption`,
//! `assert_redundancy_outcome`, `assert_distinct_domains`) live in the scenario
//! test file (`crates/chunkstore-grpc/tests/tier2_kill_reconstruct.rs`) where they
//! are called by the scenario test and covered by non-`#[ignore]`d unit tests running
//! at Check — the born-at-tier seam is load-bearing from that crate.

/// Number of D-server containers the Tier-2 kill-and-reconstruct cluster stands up:
/// [`crate::DSERVER_COUNT`] (nine, for the RS(6,3) initial placement) plus **one spare**
/// server that reconstruction re-places the rebuilt fragment onto after the victim is
/// killed. The spare (index [`crate::DSERVER_COUNT`]) is in a distinct failure domain
/// from every initial server so the post-reconstruction placement spans N distinct
/// failure domains.
pub(crate) const KR_DSERVER_COUNT: usize = crate::DSERVER_COUNT + 1;

/// Select the kill-victim by 0-indexed server index. Deterministic: always selects
/// server 0 (the first D server). Pure so unit tests can assert it without a container.
///
/// The simplest deterministic policy — "always the first" — is sufficient because the
/// harness's invariant (full reconstruction over the surviving fleet) is server-index-
/// agnostic; the Tier-0 DST already seeds-varies the kill index over all `N` servers
/// (`crates/dst/tests/custodian.rs:529-530`). The Tier-2 harness proves the same
/// invariant holds over **real gRPC D-server containers**.
pub(crate) fn select_victim_index(_server_count: usize) -> usize {
    0
}

/// Produce the Docker container name for the kill-victim. Docker Compose V2 names
/// replicas `<project>-<service>-<1-indexed-replica>`, so 0-indexed victim index 0
/// maps to replica 1 of the `dserver` service under [`crate::TIER2_PROJECT`].
pub(crate) fn victim_container_name(victim_index: usize) -> String {
    format!("{}-dserver-{}", crate::TIER2_PROJECT, victim_index + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- select_victim_index ----

    #[test]
    fn victim_is_always_server_zero() {
        // The policy is deterministic: always the first server (index 0).
        assert_eq!(select_victim_index(4), 0);
        assert_eq!(select_victim_index(9), 0);
        assert_eq!(select_victim_index(10), 0);
    }

    // ---- victim_container_name ----

    #[test]
    fn container_name_is_one_indexed() {
        // Docker Compose V2 names replicas starting at 1 (0-indexed victim 0 → replica 1).
        assert_eq!(victim_container_name(0), "wyrd-tier2-dserver-1");
        assert_eq!(victim_container_name(1), "wyrd-tier2-dserver-2");
        assert_eq!(victim_container_name(2), "wyrd-tier2-dserver-3");
    }
}
