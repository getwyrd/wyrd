//! **The live checked-consistency run under a #407 nemesis leg** (#408, slice 5 of #329;
//! ADR-0041 "nemesis first, then the checked artifact"). Hosts the production server + S3
//! gateway on a loopback listener — the composition `s3_http_wire.rs`'s `start_gateway` and
//! #405's `consistency_observable.rs` test already mirror — backed by the real
//! `FdbMetadataStore` pointed at the live `deploy/fdb-multi-replica` cluster
//! (`crates/server/src/cli.rs:138,156`, `crates/server/Cargo.toml:31`). It drives the #406
//! checked workload in the **reshaped pools** (Design §2) through concurrent clients, injects a
//! #407 nemesis leg mid-window via `wyrd_metadata_fault_conformance::nemesis::drive_leg` —
//! consumed **as-is**, never reopening its lifecycle logic (the brief's Ordering note) — and
//! writes the Elle-EDN histories plus a machine-readable **run summary** under
//! `$WYRD_CONSISTENCY_OUTPUT_DIR` (`target/consistency-run/` by default): the seam
//! `xtask::consistency_run` (Design §1) derives the non-vacuity gate / elle-cli verdict / report
//! from, entirely off this file.
//!
//! # The reshaped pools — exclusion by construction (Design §2)
//!
//! * **Register overwrite pool (Elle-fed):** a sole writer overwriting a shared key (unique
//!   version per write) and a concurrent reader — PUT/GET only, **no DELETE** (a delete has no
//!   faithful rw-register encoding), so `MultiProcessHistory::to_elle_edn` never sees a delete.
//! * **Register delete pool (Wyrd-checked):** PUT/GET/DELETE traffic on a key set **disjoint from
//!   the Elle-fed pool's**, judged by the landed INV-1-sound #406 checks
//!   (`session_read_your_writes` — which carries the resurrection / lost-write logic —,
//!   `session_monotonic_reads`, `reads_monotone_per_key`), its verdicts and per-op-kind counts
//!   recorded in the run summary. It is **never** serialized into the Elle register EDN: that is
//!   what lets the delete traffic exist at all, since the `rw-register` model cannot represent a
//!   delete (Design §2). Disjointness from the register pool is what makes the exclusion sound —
//!   Elle partitions per key, so excluding whole keys fabricates no order, whereas filtering
//!   individual ops out of a shared key's history would. **Within** the pool each process owns its
//!   own key (`consistency_workload::delete_pool_key`): the checks compare client-assigned version
//!   tags, which track commit order only under a single writer, so a shared key would make a
//!   correct system report a violation.
//! * **Directory create pool (Elle-fed, `set`):** create-only unique members, each assigned a
//!   unique **integer** id (the name↔id map goes into the summary); after heal **and a quiesce**
//!   ([`QUIESCE_AFTER_HEAL`]) the scenario probes every member of the known universe and emits ONE
//!   composed full-set `:read` — re-probing an unresolved member and, if it stays unresolved,
//!   degrading that composed read to `:info` rather than omitting the member (which the `set` model
//!   would read as a lost element). Deletes/probes never enter the set EDN.
//!
//! The INV-2 witness (`is_genuinely_concurrent`) binds to the Elle-fed register pool — the
//! non-vacuity gate attests concurrency where the verdict is claimed.
//!
//! # The #407 typed materialization evidence — carried, not asserted (T2)
//!
//! The run summary carries the leg's OWN [`MaterializationEvidence`]
//! (`kind()`/`materialized()`/`diagnosis()`), sampled by [`NemesisLeg::confirm_materialized`]
//! **inside the fault window** (the `drive_leg` workload closure). It records *what the leg
//! observed* — which fault, on which target, how it provably bit — into `run-summary.json` and
//! the report, instead of a hard-coded `nemesis_materialized = true` boolean.
//!
//! **Off-Check by construction.** `#[ignore]`d, `fdb`-feature-gated, and env-gated: absent
//! `WYRD_FDB_CLUSTER_FILE` it skips cleanly (mirrors
//! `crates/metadata-fdb/tests/tier1_metadata_nemesis.rs`'s clean-skip shape), so `cargo xtask ci`
//! stays green with no FDB, no Docker, no JVM. Launched only by `WYRD_TIER1=1 cargo xtask
//! consistency-run` (`xtask/src/consistency_run_runner.rs`); this file never shells `java`.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): the checked-consistency leg records REAL
// wall-clock op windows for the Elle checker against a live cluster, like
// server::consistency_observable (#619).
// File scope (not per-site) is deliberate here: a test crate never
// ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

/// The FDB cluster file, or `None` when FDB is not configured (clean-skip gate — mirrors
/// `tier1_metadata_nemesis.rs::cluster_file`).
fn cluster_file() -> Option<String> {
    match std::env::var("WYRD_FDB_CLUSTER_FILE") {
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => None,
    }
}

#[test]
#[ignore = "privileged #408: live 3-process FDB cluster + a #407 nemesis leg (cargo xtask consistency-run)"]
fn consistency_run_fdb() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!(
            "wyrd-server: WYRD_FDB_CLUSTER_FILE not set — skipping the #408 checked consistency \
             run."
        );
        return;
    };
    run(cluster_file);
}

/// Without `--features fdb` the crate cannot link `libfdb_c`, so the live body is compiled out;
/// this stub keeps the test binary building under the default `cargo xtask ci` gate and skips
/// loudly if a cluster file is somehow set (mirrors `tier1_metadata_nemesis.rs`'s
/// `skip_without_fdb`).
#[cfg(not(feature = "fdb"))]
fn run(cluster_file: String) {
    let _ = cluster_file;
    eprintln!(
        "wyrd-server: WYRD_FDB_CLUSTER_FILE is set but the crate was built without `--features \
         fdb` — skipping the #408 checked consistency run. Run via `cargo xtask consistency-run`."
    );
}

/// How long the run lets the cluster settle after `drive_leg` heals the fault, before the composed
/// full-set read (Design §2's "after heal + quiesce"). Healing removes the fault; FDB still has to
/// re-form its transaction system and re-replicate afterwards, and a probe issued into that window
/// comes back indeterminate — which the sweep now reports honestly (an `:info` composed read ⇒
/// INCONCLUSIVE) instead of fabricating. So this window buys a *conclusive* run, not a sound one:
/// soundness is the sweep's, whatever this is set to.
#[cfg(feature = "fdb")]
const QUIESCE_AFTER_HEAL: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(feature = "fdb")]
fn run(cluster_file: String) {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;

    use wyrd_chunkstore_fs::FsChunkStore;
    use wyrd_coordination_mem::MemCoordination;
    use wyrd_gateway_s3::sigv4::Credentials;
    use wyrd_gateway_s3::{S3Config, S3Gateway};
    use wyrd_metadata_fault_conformance::nemesis::{ClockSkewLeg, PartitionLeg, ProcessPauseLeg};
    use wyrd_metadata_fdb::FdbMetadataStore;
    use wyrd_server::Gateway;

    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    type Backend = Gateway<FdbMetadataStore, FsChunkStore, MemCoordination>;

    fn required_env(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| {
            panic!("{name} must be set by the runner (cargo xtask consistency-run)")
        })
    }

    let leg_kind = std::env::var("WYRD_CONSISTENCY_NEMESIS")
        .unwrap_or_else(|_| "network-partition".to_string());
    let target_addr = required_env("WYRD_CONSISTENCY_TARGET_ADDR");
    let target_ip = required_env("WYRD_CONSISTENCY_TARGET_IP");
    let target_service = required_env("WYRD_CONSISTENCY_TARGET_SERVICE");
    let target_container = required_env("WYRD_CONSISTENCY_TARGET_CONTAINER");
    let survivor_container = required_env("WYRD_CONSISTENCY_SURVIVOR_CONTAINER");
    let iptables_image = std::env::var("WYRD_CONSISTENCY_IPTABLES_IMAGE")
        .unwrap_or_else(|_| "wyrd-iptables:local".to_string());
    let compose_file = required_env("WYRD_CONSISTENCY_COMPOSE_FILE");
    let faketime_override = required_env("WYRD_CONSISTENCY_FAKETIME_OVERRIDE");
    let output_dir = PathBuf::from(required_env("WYRD_CONSISTENCY_OUTPUT_DIR"));
    std::fs::create_dir_all(&output_dir).expect("create the consistency-run output dir");

    let target_desc = format!("{target_service} ({target_container} @ {target_addr})");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // ── Bring-up: the production server + S3 gateway on loopback, backed by the real
    // FdbMetadataStore (the `s3_http_wire.rs` / #405 composition, over the live cluster).
    let addr: SocketAddr = runtime.block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let prefix = format!("wyrd-consistency-run/{}/", std::process::id()).into_bytes();
        let store = FdbMetadataStore::open(&cluster_file)
            .expect("open the FoundationDB metadata store")
            .with_prefix(prefix);
        let gateway: Arc<Backend> = Arc::new(Gateway::new(
            store,
            FsChunkStore::open(dir.path()).expect("fs store"),
            MemCoordination::new(),
        ));
        let config = S3Config::new(vec![Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
        }]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = S3Gateway::new(Arc::clone(&gateway), config);
        tokio::spawn(async move {
            server.serve(listener).await.expect("serve");
        });
        std::mem::forget(dir);
        addr
    });

    // Drive the fault window (all three pools run WHILE the fault is active), sampling the leg's
    // OWN typed evidence inside the window (T2). The composed final read is done POST-heal
    // (Design §2: a mid-run probe cannot compose an atomic set read), so `drive_leg` returns the
    // created universe and this function sweeps it after heal.
    let (nemesis, pools) = match leg_kind.as_str() {
        "network-partition" | "partition" => {
            let leg = PartitionLeg::new(
                target_addr,
                target_ip,
                target_container,
                survivor_container,
                iptables_image,
            );
            drive_with_evidence(&leg, &runtime, addr, &target_desc)
        }
        "clock-skew" => {
            let floor_secs = std::env::var("WYRD_CONSISTENCY_SKEW_FLOOR_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60);
            let leg = ClockSkewLeg::new(
                compose_file,
                faketime_override,
                target_service,
                target_container,
                survivor_container,
                floor_secs,
            );
            drive_with_evidence(&leg, &runtime, addr, &target_desc)
        }
        "process-pause" => {
            let leg = ProcessPauseLeg::new(target_addr, target_container, survivor_container);
            drive_with_evidence(&leg, &runtime, addr, &target_desc)
        }
        other => panic!(
            "unknown WYRD_CONSISTENCY_NEMESIS leg `{other}` (expected `network-partition` \
             [default], `clock-skew`, or `process-pause`)"
        ),
    };

    let PoolResults {
        register,
        deletes,
        creates,
        universe,
    } = pools;

    // ── Quiesce (Design §2), then the composed full-set read. `drive_leg` has healed the fault by
    // the time it returns, but healed ≠ settled: FDB still has to re-form its transaction system
    // and re-replicate, and a probe issued into that window comes back indeterminate. The sweep
    // handles an unresolved probe honestly (it degrades the composed read to `:info`), so this
    // wait is not what makes the run sound — it is what keeps the run from being pointlessly
    // inconclusive. The composed read is sound only because the set is no longer mutating: every
    // create pool has joined, and nothing writes the directory after this point.
    std::thread::sleep(QUIESCE_AFTER_HEAL);

    let final_read = runtime.block_on(sweep_final_read(addr, &universe));
    let directory = wyrd_server::consistency_workload::DirectoryHistory::from_set_run(
        creates.clone(),
        Some(final_read.clone()),
    );

    let genuinely_concurrent = register.is_genuinely_concurrent();
    let register_ops = register.ops().len();
    // The CHECKED history's size: one op per create plus the ONE composed read. The universe sweep
    // probes are the composed read's raw material, not ops in the checked history — counting them
    // (as this did) overstated the directory history ~2x in the report's "history size" field.
    let directory_ops = directory.op_count();

    // ── The Wyrd-checked delete pool's judgment (Design §2): the landed #406 checks are what
    // judge the delete traffic the `rw-register` model cannot represent. They are INV-1-sound —
    // each skips indeterminate ops rather than deriving a definite obligation — so a `false` here
    // is a real violation observed on the live cluster, not an artifact of the fault. The verdicts
    // cross the seam in the summary; the runner decides the run on them (a pool that is driven and
    // then not judged would be theatre).
    let delete_pool_ops = deletes.ops().len();
    let delete_pool_checks = serde_json::json!({
        "session_read_your_writes": deletes.session_read_your_writes(),
        "session_monotonic_reads": deletes.session_monotonic_reads(),
        "reads_monotone_per_key": deletes.reads_monotone_per_key(),
    });

    std::fs::write(
        output_dir.join("register-history.edn"),
        register.to_elle_edn(),
    )
    .expect("write register-history.edn");
    std::fs::write(
        output_dir.join("directory-history.edn"),
        directory.to_elle_edn(),
    )
    .expect("write directory-history.edn");

    let register_outcomes = register_outcome_counts(&register);
    let directory_outcomes = create_outcome_counts(&creates);
    let delete_pool_outcomes = register_outcome_counts(&deletes);
    let member_map: Vec<serde_json::Value> = universe
        .iter()
        .map(|(name, id)| serde_json::json!({ "member": name, "id": id }))
        .collect();

    let workload_description = "register overwrite PUT/GET (Elle-fed: 1 writer + 1 reader, shared \
         key) + register PUT/GET/DELETE (Wyrd-checked: 2 processes, disjoint key, judged by the \
         #406 session/monotonicity checks) + directory create-only unique integer members (set \
         model), over the S3 wire against the live FDB cluster; a composed post-heal full-set read \
         closes the directory history";
    let summary = serde_json::json!({
        "workload": workload_description,
        "nemesis": {
            "kind": nemesis.kind,
            "target": nemesis.target,
            "materialized": nemesis.materialized,
            "diagnosis": nemesis.diagnosis,
        },
        "genuinely_concurrent": genuinely_concurrent,
        "register_ops": register_ops,
        "directory_ops": directory_ops,
        "register_outcomes": register_outcomes,
        "directory_outcomes": directory_outcomes,
        "delete_pool": {
            "ops": delete_pool_ops,
            "outcomes": delete_pool_outcomes,
            "checks": delete_pool_checks,
        },
        "member_id_map": member_map,
        "composed_final_read": final_read.present,
        // The composed read's honesty, across the seam: a sweep that could not resolve every
        // member composed no definite set, so the run cannot claim a directory verdict. The
        // runner's gate reads this and says so precisely (the real checker independently returns
        // `:unknown` for the `:info` read this emits — the gate makes the REASON legible).
        "composed_final_read_determinate": final_read.is_determinate(),
        "composed_final_read_unresolved": final_read.unresolved,
    });
    std::fs::write(
        output_dir.join("run-summary.json"),
        serde_json::to_string_pretty(&summary).expect("serialize run summary"),
    )
    .expect("write run-summary.json");

    assert!(
        nemesis.materialized,
        "the leg's typed evidence must attest a materialized fault by the time the workload ran: {}",
        nemesis.diagnosis
    );
    assert!(
        genuinely_concurrent,
        "the checked register overwrite pool produced NO genuinely concurrent (#406 INV-2) overlap \
         — the run summary would be vacuous and the runner's non-vacuity gate would refuse a \
         verdict; register ops recorded: {register_ops}"
    );
    assert!(
        register_ops > 0 && !creates.is_empty() && delete_pool_ops > 0,
        "every pool must be non-empty (register: {register_ops}, creates: {}, delete pool: \
         {delete_pool_ops})",
        creates.len()
    );
}

/// A leg-agnostic snapshot of the #407 typed materialization evidence.
#[cfg(feature = "fdb")]
struct NemesisEvidenceRecord {
    kind: String,
    target: String,
    materialized: bool,
    diagnosis: String,
}

#[cfg(feature = "fdb")]
impl NemesisEvidenceRecord {
    fn from_evidence<E>(evidence: &E, target: &str) -> Self
    where
        E: wyrd_metadata_fault_conformance::nemesis::MaterializationEvidence,
    {
        Self {
            kind: evidence.kind().as_str().to_string(),
            target: target.to_string(),
            materialized: evidence.materialized(),
            diagnosis: evidence.diagnosis(),
        }
    }
}

/// What the three pools (Design §2) produced inside one fault window.
#[cfg(feature = "fdb")]
struct PoolResults {
    /// The Elle-fed register **overwrite** pool's merged history — PUT/GET only, serialized to
    /// the `rw-register` EDN, and the pool the INV-2 concurrency witness binds to.
    register: wyrd_server::consistency_workload::MultiProcessHistory,
    /// The **Wyrd-checked delete pool**'s merged history over a DISJOINT key set — PUT/GET/DELETE,
    /// judged by the #406 checks, counted in the summary, never serialized to the register EDN.
    deletes: wyrd_server::consistency_workload::MultiProcessHistory,
    /// The Elle-fed directory create pool's `:add`s (integer elements).
    creates: Vec<wyrd_server::consistency_workload::DirCreate>,
    /// The created directory universe (name↔id), swept post-heal into the composed `:read`.
    universe: Vec<(String, u64)>,
}

/// Drive one nemesis leg end-to-end via `drive_leg` (consumed as-is), capturing the leg's OWN
/// typed materialization evidence INSIDE the fault window (T2), plus every pool's result.
#[cfg(feature = "fdb")]
fn drive_with_evidence<L>(
    leg: &L,
    runtime: &tokio::runtime::Runtime,
    addr: std::net::SocketAddr,
    target_desc: &str,
) -> (NemesisEvidenceRecord, PoolResults)
where
    L: wyrd_metadata_fault_conformance::nemesis::NemesisLeg,
{
    use wyrd_metadata_fault_conformance::nemesis::{drive_leg, NemesisLeg};
    drive_leg(leg, || {
        let evidence = <L as NemesisLeg>::confirm_materialized(leg).expect(
            "re-sample the nemesis materialization evidence inside the fault window (the leg \
             already gated materialization, so this returns fast)",
        );
        let record = NemesisEvidenceRecord::from_evidence(&evidence, target_desc);
        let pools = runtime.block_on(drive_pools(addr));
        (record, pools)
    })
    .expect("nemesis leg must materialize, drive the checked workload, and heal completely")
}

/// Drive the three reshaped pools over the real S3 wire — called from INSIDE the nemesis fault
/// window so every op genuinely races the fault (Design §2).
///
/// **Every op is kept, including the ones the fault breaks.** An op whose transport failed is
/// recorded by the client as indeterminate (`consistency_observable::INDETERMINATE_STATUS` ⇒
/// `:info`), so ignoring the `Result` here discards only a redundant error value — never the op
/// itself. Dropping those ops would delete precisely the evidence the nemesis exists to produce
/// and hand the checker a history that looks like an unremarkable clean run.
#[cfg(feature = "fdb")]
async fn drive_pools(addr: std::net::SocketAddr) -> PoolResults {
    use wyrd_gateway_s3::sigv4::Credentials;
    use wyrd_server::consistency_observable::ObservableS3Client;
    use wyrd_server::consistency_workload::{
        DirCreate, MultiProcessHistory, REGISTER_OVERWRITE_POOL_KEY,
    };

    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const REGION: &str = "us-east-1";
    const BUCKET: &str = "wyrd-consistency-run";
    const ROUNDS: u64 = 60;

    fn client(addr: std::net::SocketAddr) -> ObservableS3Client {
        let creds = Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
        };
        ObservableS3Client::new(addr, BUCKET, creds, REGION)
    }

    // ── Register overwrite pool (Elle-fed): process 0 the sole writer overwriting a shared key,
    // process 1 a concurrent reader — PUT/GET only, no DELETE (Design §2).
    let register_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let writer = {
        let mut c = client(addr);
        let barrier = register_barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            for v in 1..=ROUNDS {
                // An error here is an indeterminate op the client has already recorded as :info.
                let _ = c.put(REGISTER_OVERWRITE_POOL_KEY, v).await;
            }
            c.into_history()
        })
    };
    let reader = {
        let mut c = client(addr);
        let barrier = register_barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..ROUNDS {
                let _ = c.get(REGISTER_OVERWRITE_POOL_KEY).await;
            }
            c.into_history()
        })
    };
    let register_histories = vec![
        writer.await.expect("writer task"),
        reader.await.expect("reader task"),
    ];
    let register = MultiProcessHistory::merge(&register_histories);

    // ── Register delete pool (Wyrd-checked): PUT/GET/DELETE over a key set DISJOINT from the
    // Elle-fed pool's, judged by the #406 checks and never serialized to the register EDN
    // (Design §2). Two processes on a shared delete-pool key so the RYW / monotonic-read /
    // resurrection logic has cross-process traffic to judge.
    let delete_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let deleter_a = spawn_delete_pool_process(addr, 0, ROUNDS, delete_barrier.clone());
    let deleter_b = spawn_delete_pool_process(addr, 1, ROUNDS, delete_barrier.clone());
    let delete_histories = vec![
        deleter_a.await.expect("delete-pool task a"),
        deleter_b.await.expect("delete-pool task b"),
    ];
    let deletes = MultiProcessHistory::merge(&delete_histories);

    // ── Directory create pool (Elle-fed, set model): two processes create-only unique members,
    // each a unique INTEGER id. Deletes/probes never enter the set EDN.
    let dir_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let creator_a = spawn_creator(addr, 0, 0, ROUNDS, dir_barrier.clone());
    let creator_b = spawn_creator(addr, 1, ROUNDS, ROUNDS, dir_barrier.clone());
    let mut creates: Vec<DirCreate> = Vec::new();
    let mut universe: Vec<(String, u64)> = Vec::new();
    for handle in [creator_a, creator_b] {
        let (recs, members) = handle.await.expect("creator task");
        creates.extend(recs);
        universe.extend(members);
    }

    PoolResults {
        register,
        deletes,
        creates,
        universe,
    }
}

/// One Wyrd-checked delete-pool process: PUT/GET/DELETE traffic on **its own** delete-pool key
/// (`consistency_workload::delete_pool_key(process)` — disjoint from every other process's and
/// from the Elle-fed [`REGISTER_OVERWRITE_POOL_KEY`]), recorded for the #406 checks and never for
/// the Elle register EDN.
///
/// **Single writer per key, by construction.** The version tag is client-assigned, so it orders by
/// *writer*, not by *commit* — and the three #406 checks judging this pool compare raw tags on a
/// key. Two writers on one key (however the versions are banded) therefore make a perfectly
/// linearizable execution report `false`, which the runner escalates to "a real violation observed
/// on the live cluster": a fabricated violation. Owning the key removes that premise; the
/// cross-process concurrency this run claims a verdict on lives in the Elle-fed overwrite pool,
/// where the real checker — not a tag comparison — judges it. See
/// `consistency_workload::delete_pool_key`'s docs and the Check-time pins in
/// `crates/server/tests/consistency_workload.rs`.
#[cfg(feature = "fdb")]
fn spawn_delete_pool_process(
    addr: std::net::SocketAddr,
    process: usize,
    rounds: u64,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
) -> tokio::task::JoinHandle<wyrd_server::consistency_observable::History> {
    use wyrd_gateway_s3::sigv4::Credentials;
    use wyrd_server::consistency_observable::ObservableS3Client;
    use wyrd_server::consistency_workload::delete_pool_key;

    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const REGION: &str = "us-east-1";
    const BUCKET: &str = "wyrd-consistency-run";

    tokio::spawn(async move {
        let creds = Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
        };
        let mut c = ObservableS3Client::new(addr, BUCKET, creds, REGION);
        let key = delete_pool_key(process);
        barrier.wait().await;
        // Ascending versions on this process's OWN key: sole writer ⇒ tag order = commit order,
        // which is exactly what the #406 checks assume.
        for v in 1..=rounds {
            // PUT -> read-your-write -> DELETE -> read-after-delete: the exact traffic
            // `session_read_your_writes` (with its resurrection / lost-write logic) and
            // `reads_monotone_per_key` judge. Every op's Result is ignored deliberately: the
            // client records an errored op as :info, and the #406 checks skip indeterminate ops
            // rather than deriving a definite obligation from them (INV-1).
            let _ = c.put(&key, v).await;
            let _ = c.get(&key).await;
            let _ = c.delete(&key).await;
            let _ = c.get(&key).await;
        }
        c.into_history()
    })
}

/// One creator task's result: the recorded `:add` creates plus the name↔id universe it added.
#[cfg(feature = "fdb")]
type CreatorResult = (
    Vec<wyrd_server::consistency_workload::DirCreate>,
    Vec<(String, u64)>,
);

/// One directory-creator process: creates `count` unique integer members starting at `id_base`,
/// recording each as a [`DirCreate`] and its name↔id in the universe.
#[cfg(feature = "fdb")]
fn spawn_creator(
    addr: std::net::SocketAddr,
    process: usize,
    id_base: u64,
    count: u64,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
) -> tokio::task::JoinHandle<CreatorResult> {
    use wyrd_gateway_s3::sigv4::Credentials;
    use wyrd_server::consistency_observable::{ObservableS3Client, OpFailed};
    use wyrd_server::consistency_workload::DirCreate;

    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const REGION: &str = "us-east-1";
    const BUCKET: &str = "wyrd-consistency-run";

    tokio::spawn(async move {
        let creds = Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
        };
        let mut c = ObservableS3Client::new(addr, BUCKET, creds, REGION);
        barrier.wait().await;
        let mut recs = Vec::new();
        let mut members = Vec::new();
        for i in 0..count {
            let id = id_base + i + 1; // integer element, unique across processes, 1-based
            let member = format!("dir/member-{id}");
            // The create's OWN record — its status and the real-time span the client measured —
            // taken from the call itself, whether it succeeded or failed at the transport (an
            // indeterminate create is recorded and returned in `OpFailed`). Never re-read off
            // `history().ops().last()`: the tail can hold a NEIGHBOUR's op, and this create would
            // then inherit that op's determinate status, serializing an indeterminate `:add` as a
            // definite `:ok` (INV-1).
            let record = c
                .put(&member, 1) // create = PUT
                .await
                .unwrap_or_else(OpFailed::into_record);
            recs.push(DirCreate {
                process,
                id,
                status: record.status,
                start: record.start,
                end: record.end,
            });
            members.push((member, id));
        }
        (recs, members)
    })
}

/// How many times a member whose probe came back [`Membership::Unknown`] is re-probed before the
/// sweep gives up on resolving it. The sweep runs after heal + quiesce, so an unknown probe is
/// most likely a straggler while the cluster settles — worth retrying — but retrying forever would
/// hang the run, so it is bounded and the residue is reported honestly rather than assumed away.
#[cfg(feature = "fdb")]
const FINAL_READ_PROBE_ATTEMPTS: usize = 5;

/// The post-heal composed full-set read (Design §2): probe every member of the known universe
/// sequentially and compose ONE atomic `:read` — sound because, post-heal and post-quiesce, the set
/// is no longer mutating.
///
/// **This function is only the I/O half.** Which observations may become a definite set is decided
/// by `consistency_workload::compose_final_read` (production, and unit-pinned at Check —
/// this file compiles only under `--features fdb`, so a decision left in here would be judged by
/// nothing).
///
/// An [`Membership::Unknown`] probe (5xx/timeout — what a nemesis induces) is **re-probed** up to
/// [`FINAL_READ_PROBE_ATTEMPTS`] times; a member still unresolved after that is handed to the
/// composer as unresolved, which degrades the whole composed read to `:info`. It is emphatically
/// NOT dropped from the sweep: in the `set` model an acknowledged `:add` missing from a definite
/// `:ok` read is a lost element, so omitting it would fabricate a `false` out of an unanswered
/// probe (verified against the real jar).
#[cfg(feature = "fdb")]
async fn sweep_final_read(
    addr: std::net::SocketAddr,
    universe: &[(String, u64)],
) -> wyrd_server::consistency_workload::DirFinalRead {
    use wyrd_gateway_s3::sigv4::Credentials;
    use wyrd_server::consistency_observable::{ObservableS3Client, OpFailed};
    use wyrd_server::consistency_workload::{compose_final_read, membership, Membership};

    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const REGION: &str = "us-east-1";
    const BUCKET: &str = "wyrd-consistency-run";

    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let mut c = ObservableS3Client::new(addr, BUCKET, creds, REGION);
    let start = std::time::SystemTime::now();
    let mut probes: Vec<(u64, u16)> = Vec::with_capacity(universe.len());
    for (member, id) in universe {
        let mut status = 0u16;
        for attempt in 0..FINAL_READ_PROBE_ATTEMPTS {
            // This probe's OWN status, from the call itself — an errored probe yields its own
            // indeterminate record, never the previous member's determinate 200, which would
            // compose a member into the final set that was never observed present (INV-1).
            let record = c.get(member).await.unwrap_or_else(OpFailed::into_record);
            status = record.status;
            if membership(status) != Membership::Unknown {
                break; // resolved: present (200) or genuinely absent (404)
            }
            if attempt + 1 < FINAL_READ_PROBE_ATTEMPTS {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
        probes.push((*id, status));
    }
    let end = std::time::SystemTime::now();
    compose_final_read(0, &probes, start, end)
}

/// Per-op-kind outcome counts over the register overwrite history (Design §4).
#[cfg(feature = "fdb")]
fn register_outcome_counts(
    history: &wyrd_server::consistency_workload::MultiProcessHistory,
) -> serde_json::Value {
    use wyrd_server::consistency_observable::OpKind;
    use wyrd_server::consistency_workload::is_indeterminate;
    let (mut ok, mut fail, mut info) = (0usize, 0usize, 0usize);
    let invoked = history.ops().len();
    for op in history.ops() {
        let status = op.record.status;
        if is_indeterminate(status) {
            info += 1;
        } else {
            let is_ok = match op.record.kind {
                OpKind::Put => (200..300).contains(&status),
                OpKind::Get | OpKind::Delete => (200..300).contains(&status) || status == 404,
            };
            if is_ok {
                ok += 1;
            } else {
                fail += 1;
            }
        }
    }
    serde_json::json!({ "invoked": invoked, "ok": ok, "fail": fail, "info": info })
}

/// Per-op-kind outcome counts over the directory creates (Design §4).
#[cfg(feature = "fdb")]
fn create_outcome_counts(
    creates: &[wyrd_server::consistency_workload::DirCreate],
) -> serde_json::Value {
    use wyrd_server::consistency_workload::is_indeterminate;
    let (mut ok, mut fail, mut info) = (0usize, 0usize, 0usize);
    for c in creates {
        if is_indeterminate(c.status) {
            info += 1;
        } else if (200..300).contains(&c.status) {
            ok += 1;
        } else {
            fail += 1;
        }
    }
    serde_json::json!({ "invoked": creates.len(), "ok": ok, "fail": fail, "info": info })
}
