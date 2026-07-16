//! **Tier-1 metadata nemesis over a real FoundationDB cluster** — the live wiring of the three
//! composable nemesis legs (#407, slice 4 of #329; ADR-0041 "nemesis first, then the checked
//! artifact") into the fdb-feature Tier-1 scenario.
//!
//! Each leg is one of `wyrd-metadata-fault-conformance`'s three fault classes
//! (`nemesis::{PartitionLeg, ClockSkewLeg, ProcessPauseLeg}`), resolved against the live
//! `deploy/fdb-multi-replica` cluster and driven by `nemesis::drive_leg`, which enforces the two
//! #442 gates: a fault that did not bite FAILS as inconclusive, and an incomplete heal FAILS.
//! The workload run *under* each fault here is a minimal production-path commit round-trip
//! through `FdbMetadataStore` — proof the majority side keeps serving under the fault. Because
//! `drive_leg` runs the workload WHILE the fault is still active (the fault encloses the
//! workload) and heals on every path (including a panicking workload), a red here is a real
//! violation, never a leaked cut. #408 composes the *checked* Elle-history workload under these
//! same legs.
//!
//! **Off-Check by construction.** These functions are `#[ignore]`d and need a live 3-process FDB
//! cluster + `libfdb_c` + privileged in-netns `iptables` (partition) + `libfaketime` (skew).
//! They run only under the opt-in Tier-1 runner (`WYRD_TIER1=1 cargo xtask metadata-nemesis`,
//! and the future #409 CI job), which is the runnable entry point that stands up the cluster,
//! resolves the topology, and exports the fault env this file reads. Absent an
//! `WYRD_FDB_CLUSTER_FILE` they skip cleanly, so `cargo xtask ci` stays green. The pure decision
//! logic they turn on is unit-tested at Check in `wyrd-metadata-fault-conformance`'s
//! `nemesis_oracles` and `xtask`'s `nemesis_orchestration`.
//!
//! The scenario-function NAMES are pinned by `xtask::nemesis::NemesisLegKind::scenario_fn`
//! (`nemesis_metadata_under_{network_partition,clock_skew,process_pause}`) — the runner selects
//! each with `--exact` and REFUSES a leg that ran zero tests
//! (`xtask::nemesis::nemesis_leg_ran_exactly_one`), so renaming one here without updating that
//! dispatch fails the leg loudly instead of passing as a silent green no-op.

#[cfg(feature = "fdb")]
mod support;

/// The FDB cluster file, or `None` when FDB is not configured (clean-skip gate).
fn cluster_file() -> Option<String> {
    match std::env::var("WYRD_FDB_CLUSTER_FILE") {
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => None,
    }
}

#[test]
#[ignore = "privileged Tier-1: live 3-process FDB cluster + in-netns iptables (cargo xtask metadata-nemesis)"]
fn nemesis_metadata_under_network_partition() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!("wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the partition nemesis leg.");
        return;
    };
    run_partition(cluster_file);
}

#[test]
#[ignore = "privileged Tier-1: live 3-process FDB cluster + libfaketime (cargo xtask metadata-nemesis)"]
fn nemesis_metadata_under_clock_skew() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!("wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the clock-skew nemesis leg.");
        return;
    };
    run_clock_skew(cluster_file);
}

#[test]
#[ignore = "privileged Tier-1: live 3-process FDB cluster + docker pause (cargo xtask metadata-nemesis)"]
fn nemesis_metadata_under_process_pause() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!("wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the process-pause nemesis leg.");
        return;
    };
    run_process_pause(cluster_file);
}

#[cfg(feature = "fdb")]
fn run_partition(cluster_file: String) {
    use std::time::Duration;
    use wyrd_metadata_fault_conformance::nemesis::{drive_leg, PartitionLeg};

    let all = support::processes().expect("WYRD_TIER1_NETNS_MAP must name the cluster processes");
    // Cut the master — an arbitrary node is outcome-neutral (FDB keeps quorum on the majority).
    let target = support::resolve_role_holder(&all, "master", Duration::from_secs(90))
        .expect("resolve the master process to cut");
    let survivor = support::survivor(&all, &target).expect("a survivor other than the target");
    let iptables_image = std::env::var("WYRD_TIER1_IPTABLES_IMAGE")
        .unwrap_or_else(|_| "wyrd-iptables:local".to_string());

    let leg = PartitionLeg::new(
        target.addr.clone(),
        target.ip.clone(),
        target.container.clone(),
        survivor.container.clone(),
        iptables_image,
    );
    drive_leg(&leg, || cluster_still_serves(&cluster_file))
        .expect("partition leg must materialize, keep the majority serving, and heal completely");
}

#[cfg(feature = "fdb")]
fn run_process_pause(cluster_file: String) {
    use std::time::Duration;
    use wyrd_metadata_fault_conformance::nemesis::{drive_leg, ProcessPauseLeg};

    let all = support::processes().expect("WYRD_TIER1_NETNS_MAP must name the cluster processes");
    let target = support::resolve_role_holder(&all, "master", Duration::from_secs(90))
        .expect("resolve the master process to pause");
    let survivor = support::survivor(&all, &target).expect("a survivor other than the target");

    let leg = ProcessPauseLeg::new(
        target.addr.clone(),
        target.container.clone(),
        survivor.container.clone(),
    );
    // `drive_leg` runs the workload WHILE the node is still paused (the pause encloses the
    // workload) and unpauses only in `heal`, then proves the serve→pause→serve recovery.
    drive_leg(&leg, || cluster_still_serves(&cluster_file)).expect(
        "pause leg must serve→freeze→serve, keep the majority serving, and heal completely",
    );
}

#[cfg(feature = "fdb")]
fn run_clock_skew(cluster_file: String) {
    use wyrd_metadata_fault_conformance::nemesis::{drive_leg, ClockSkewLeg};

    // Service, container, compose file and override are all resolved by the runner
    // (`fdb_faults::run_metadata_nemesis`) from ONE service name, so the recreate, the compose
    // override, and the in-container probe all agree on which node is skewed — no default-run
    // triple-mismatch. Crucially the runner exports `WYRD_TIER1_SKEW_CONTAINER` as the STABLE
    // compose container NAME (`fdb_faults::container_name_of`), not an ephemeral id: the leg's
    // `apply()` force-recreates the node, which mints a new id but keeps the name, so the
    // post-recreate `docker exec <container> date +%s` probe still resolves. The recreate restarts
    // AND (no `volumes:`) wipes the node, so `apply`/`heal` poll a SURVIVOR's `status json` until
    // the cluster fully re-replicates before the measured workload opens — the leg measures skew,
    // not the restart, and no "non-master" precondition is needed.
    let service = std::env::var("WYRD_TIER1_SKEW_SERVICE")
        .expect("WYRD_TIER1_SKEW_SERVICE must name the node the faketime override targets");
    let target_container = std::env::var("WYRD_TIER1_SKEW_CONTAINER").expect(
        "WYRD_TIER1_SKEW_CONTAINER must be the STABLE compose container NAME of \
         WYRD_TIER1_SKEW_SERVICE (stable across the leg's force-recreate)",
    );
    let survivor_container = std::env::var("WYRD_TIER1_SKEW_SURVIVOR").expect(
        "WYRD_TIER1_SKEW_SURVIVOR must be the STABLE compose container NAME of a survivor node \
         (other than the skew target) whose `status json` reports cluster recovery",
    );
    let compose_file = std::env::var("WYRD_TIER1_COMPOSE_FILE").expect(
        "WYRD_TIER1_COMPOSE_FILE must point at deploy/fdb-multi-replica/docker-compose.yml",
    );
    let faketime_override = std::env::var("WYRD_TIER1_FAKETIME_OVERRIDE")
        .expect("WYRD_TIER1_FAKETIME_OVERRIDE must point at docker-compose.faketime.yml");
    let floor_secs = std::env::var("WYRD_TIER1_SKEW_FLOOR_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(60);

    let leg = ClockSkewLeg::new(
        compose_file,
        faketime_override,
        service,
        target_container,
        survivor_container,
        floor_secs,
    );
    drive_leg(&leg, || cluster_still_serves(&cluster_file))
        .expect("clock-skew leg must offset the node clock past the floor, keep serving, and heal");
}

/// Without `--features fdb` the crate cannot link `libfdb_c`, so the leg bodies are compiled out;
/// these stubs keep the test binary building under the default `cargo xtask ci` gate and skip
/// loudly if a cluster file is somehow set. (Same shape as `tier1_metadata_consistency`'s `run`
/// stub.)
#[cfg(not(feature = "fdb"))]
fn run_partition(cluster_file: String) {
    skip_without_fdb(cluster_file);
}
#[cfg(not(feature = "fdb"))]
fn run_clock_skew(cluster_file: String) {
    skip_without_fdb(cluster_file);
}
#[cfg(not(feature = "fdb"))]
fn run_process_pause(cluster_file: String) {
    skip_without_fdb(cluster_file);
}
#[cfg(not(feature = "fdb"))]
fn skip_without_fdb(cluster_file: String) {
    let _ = cluster_file;
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE is set but the crate was built without \
         `--features fdb` — skipping the nemesis legs. Run via `cargo xtask metadata-nemesis`."
    );
}

/// The minimal production-path workload run UNDER each fault: a commit round-trip through the
/// real `FdbMetadataStore`, proving the majority side keeps serving. #408 replaces this with the
/// checked Elle-history workload.
#[cfg(feature = "fdb")]
fn cluster_still_serves(cluster_file: &str) {
    use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async {
        let prefix = format!("wyrd-nemesis/{}/", std::process::id()).into_bytes();
        let store = wyrd_metadata_fdb::FdbMetadataStore::open(cluster_file)
            .expect("open the FoundationDB metadata store")
            .with_prefix(prefix);
        let key = b"probe".to_vec();
        assert_eq!(
            store
                .commit(WriteBatch::new().put(key.clone(), b"served".to_vec()))
                .await
                .expect("a commit on the majority side must not fault under the nemesis"),
            CommitOutcome::Committed,
            "the majority side must keep serving commits under the nemesis leg",
        );
        store
            .commit(WriteBatch::new().delete(key))
            .await
            .expect("cleanup must not fault");
    });
}
