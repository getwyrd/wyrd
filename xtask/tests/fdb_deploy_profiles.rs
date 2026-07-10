//! FoundationDB deploy-profile parity guard (issue #469).
//!
//! TiKV has a `deploy/` recipe at all three ADR-0043 fixture tiers (single-node,
//! multi-replica, single-zone "small multi-node"); FoundationDB — the *chosen*
//! production metadata backend (ADR-0042) — had only the single-node testbed. This
//! guard pins the two new FDB stacks that bring it to parity, plus the `deploy/README.md`
//! profile matrix that names the single-zone pair explicitly (the rename of
//! `small-multi-node/` → `small-multi-node-tikv/` is deferred, so the pairing is recorded
//! in prose).
//!
//! Two kinds of check, split so the binding RED does **not** depend on Docker:
//!
//! **(1) Unconditional (pure filesystem, no Docker).** The two new compose files exist;
//! the FDB single-zone stack wires **every** metadata role (the 3 custodians + 3 gateways)
//! to `--metadata-backend fdb` and **no** role to `tikv`; and `deploy/README.md` names all
//! six profiles, states which single-zone setup is canonical, and records the
//! `small-multi-node/` ⇄ `small-multi-node-fdb/` pairing. RED pre-fix by non-existence.
//!
//! **(2) Behind `docker_compose_available()`** (the convention from
//! `deploy_no_orchestrator_coupling.rs`: hard failure in CI, warn-and-skip locally):
//! `docker compose config` parses each new stack and it declares the required roles — the
//! 3 `fdbserver` processes plus the fault sidecar for `fdb-multi-replica/`; the 3-node
//! etcd ensemble, the FDB cluster, 9 D servers, the custodian role and the S3-gateway role
//! for `small-multi-node-fdb/`. This only *parses* the compose files, never brings up a
//! container. A NEW file: it does not extend `deploy_no_orchestrator_coupling.rs`, whose
//! two existing signals keep gating byte-unchanged.

use std::path::{Path, PathBuf};
use std::process::Command;

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

fn exists(rel: &str) -> bool {
    workspace_root().join(rel).exists()
}

// ─── (1) unconditional, pure-filesystem parity checks ─────────────────────────

#[test]
fn both_new_fdb_stacks_exist() {
    assert!(
        exists("deploy/fdb-multi-replica/docker-compose.yml"),
        "deploy/fdb-multi-replica/docker-compose.yml is missing — FDB has no ≥3-process \
         cluster for #442's fault battery to run against"
    );
    assert!(
        exists("deploy/small-multi-node-fdb/docker-compose.yml"),
        "deploy/small-multi-node-fdb/docker-compose.yml is missing — the production track \
         has never been stood up at the single-zone topology"
    );
}

#[test]
fn fdb_single_zone_wires_every_metadata_role_to_fdb_and_none_to_tikv() {
    // The tautology trap: a guard that only asked `contains("--metadata-backend") &&
    // contains("fdb")` passes even if the three gateways were flipped to `tikv`, because
    // "fdb" appears dozens of times via image/service/volume names. Instead, count the
    // `--metadata-backend <backend>` PAIRS directly (the roles use the JSON-array command
    // form `[..., "--metadata-backend", "fdb", ...]`, so the flag and its value are one
    // literal): EVERY occurrence must name `fdb`, and NONE may name `tikv`. This mirrors
    // the TiKV peer, where exactly the 3 custodians + 3 S3 gateways open the metadata
    // backend and the 9 D servers open none (deploy/small-multi-node/docker-compose.yml
    // :360 custodian, :393 gateway).
    let compose = read("deploy/small-multi-node-fdb/docker-compose.yml");
    let total = compose.matches("\"--metadata-backend\"").count();
    let fdb = compose.matches("\"--metadata-backend\", \"fdb\"").count();
    let tikv = compose.matches("\"--metadata-backend\", \"tikv\"").count();

    assert!(
        total >= 6,
        "expected ≥6 roles opening a metadata backend (3 custodians + 3 S3 gateways), \
         found {total} — the FDB single-zone stack must mirror the TiKV peer's role set"
    );
    assert_eq!(
        fdb,
        total,
        "every `--metadata-backend` in the FDB single-zone stack must name `fdb`; \
         {} of {total} do not — a role opening a different backend is a wiring bug",
        total - fdb
    );
    assert_eq!(
        tikv, 0,
        "no role in the FDB single-zone stack may open the `tikv` metadata backend; \
         found {tikv}"
    );
}

/// The single-zone FDB stack is the **persistent** tier (unlike the throwaway
/// `fdb-multi-replica/` fault stack), so its metadata must survive a `compose down && up`.
/// The image runs `fdbserver --datadir /var/fdb/data`, so each of the three processes needs
/// its OWN named volume there — otherwise the cluster DB lives in the container writable
/// layer and a recreate silently discards it while every peer (etcd / D servers / gateways,
/// and the TiKV sibling's pd*/tikv* dirs) survives. Pure source read, so it runs without
/// Docker.
#[test]
fn each_single_zone_fdbserver_persists_its_data_directory() {
    let compose = read("deploy/small-multi-node-fdb/docker-compose.yml");
    for process in ["fdb0", "fdb1", "fdb2"] {
        let mount = format!("{process}-data:/var/fdb/data");
        assert!(
            compose.contains(&mount),
            "small-multi-node-fdb: `{process}` must mount `{mount}` — without it a \
             `compose down && up` discards the cluster metadata while every peer persists"
        );
        // …and the volume it names is actually declared, or `docker compose` rejects it.
        assert!(
            compose.contains(&format!("{process}-data:\n")),
            "small-multi-node-fdb: the named volume `{process}-data` is used but not declared"
        );
    }
}

#[test]
fn readme_profile_matrix_names_all_six_profiles() {
    let readme = read("deploy/README.md");
    // The two metadata backends × the three ADR-0043 fixture tiers. `small-multi-node/`
    // carries the trailing slash so it is not satisfied by the `small-multi-node-fdb`
    // substring.
    for profile in [
        "tikv-single-node",
        "tikv-multi-replica",
        "small-multi-node/",
        "fdb-single-node",
        "fdb-multi-replica",
        "small-multi-node-fdb",
    ] {
        assert!(
            readme.contains(profile),
            "deploy/README.md's profile matrix does not name `{profile}` — all six \
             profiles (TiKV/FDB × single-node/multi-replica/small-multi-node) must appear"
        );
    }
}

#[test]
fn readme_states_which_single_zone_stack_is_canonical() {
    let readme = read("deploy/README.md");
    assert!(
        readme.contains("currently canonical"),
        "deploy/README.md must state which small-multi-node setup is currently canonical \
         (TiKV until #442 records go, FDB after)"
    );
}

#[test]
fn readme_records_the_tikv_fdb_single_zone_pairing() {
    // Because the rename is deferred, the README is where the pairing is made explicit:
    // `small-multi-node/` IS the TiKV peer of `small-multi-node-fdb/`, so the unqualified
    // name is not read as "the" stack (issue #469's "two clearly-named peer setups",
    // discharged in prose).
    let readme = read("deploy/README.md");
    assert!(
        readme.contains("is the TiKV peer of"),
        "deploy/README.md must record that `small-multi-node/` is the TiKV peer of \
         `small-multi-node-fdb/` (the deferred-rename pairing statement)"
    );
    assert!(
        readme.contains("small-multi-node-fdb"),
        "deploy/README.md must name the FDB single-zone peer `small-multi-node-fdb`"
    );
}

// ─── (2) `docker compose config` structural validity of the new stacks ────────

/// Is a working `docker compose` CLI reachable? Mirrors `docker_compose_available` in
/// `xtask/tests/deploy_no_orchestrator_coupling.rs` (a per-file helper by the same
/// convention — a hard failure in CI, warn-and-skip locally).
fn docker_compose_available() -> bool {
    Command::new("docker")
        .args(["compose", "version"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn is_ci() -> bool {
    std::env::var_os("CI").is_some()
}

/// Run `docker compose [--profile <p>] -f <compose> config` and return the rendered,
/// normalized config text, asserting it parsed cleanly.
fn compose_config(rel: &str, profile: Option<&str>) -> String {
    let compose = workspace_root().join(rel);
    let mut cmd = Command::new("docker");
    cmd.arg("compose");
    if let Some(p) = profile {
        cmd.args(["--profile", p]);
    }
    cmd.arg("-f").arg(&compose).arg("config");
    let output = cmd
        .output()
        .expect("failed to spawn `docker compose config`");
    assert!(
        output.status.success(),
        "`docker compose -f {} config` must parse cleanly:\nstdout: {}\nstderr: {}",
        compose.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn skip_or_fail_without_docker() -> bool {
    if docker_compose_available() {
        return false;
    }
    assert!(
        !is_ci(),
        "docker compose is not available but is required in CI \
         (see .github/workflows/ci.yml's ubuntu-latest runner)"
    );
    eprintln!(
        "warning: docker compose not available; skipping the FDB deploy-profile \
         compose-config validity checks locally. Install Docker (and the compose plugin) \
         to run them."
    );
    true
}

#[test]
fn fdb_multi_replica_declares_three_processes_and_the_fault_sidecar() {
    if skip_or_fail_without_docker() {
        return;
    }
    // `--profile fault` surfaces the on-demand sidecar (behind that profile so a plain
    // `up` never starts it).
    let merged = compose_config("deploy/fdb-multi-replica/docker-compose.yml", Some("fault"));

    // Three `fdbserver` processes (a single-process cluster cannot exhibit #442's
    // replica-loss / mid-commit-kill faults).
    for service in ["fdb0", "fdb1", "fdb2"] {
        assert!(
            merged.contains(service),
            "fdb-multi-replica must declare three fdbserver processes; `{service}` is \
             missing:\n{merged}"
        );
    }
    assert!(
        merged.contains("foundationdb/foundationdb:"),
        "fdb-multi-replica's processes must run the pinned FoundationDB image:\n{merged}"
    );
    // The fault sidecar, reused as-is from tikv-multi-replica/iptables-agent/.
    assert!(
        merged.contains("iptables-agent"),
        "fdb-multi-replica must declare the fault sidecar service:\n{merged}"
    );
    assert!(
        merged.contains("wyrd-iptables:local"),
        "fdb-multi-replica's fault sidecar must build/tag the reused `wyrd-iptables:local` \
         image:\n{merged}"
    );
}

#[test]
fn small_multi_node_fdb_compose_config_is_structurally_valid() {
    if skip_or_fail_without_docker() {
        return;
    }
    let merged = compose_config("deploy/small-multi-node-fdb/docker-compose.yml", None);

    // The identical role set to the TiKV single-zone peer, metadata tier swapped: the
    // 3-node etcd ensemble, the 3-process FDB cluster, the 9 D servers (fd0..fd8 —
    // `dserver8` proves the 9th is declared), the custodian role, and the S3 gateway role.
    for service in [
        "etcd0",
        "etcd1",
        "etcd2",
        "fdb0",
        "fdb1",
        "fdb2",
        "dserver0",
        "dserver8",
        "custodian0",
        "gateway0",
    ] {
        assert!(
            merged.contains(service),
            "small-multi-node-fdb is missing the `{service}` service — every role (etcd / \
             FDB / D server / custodian / S3 gateway) must be declared:\n{merged}"
        );
    }
    // The pinned external images and #470's feature-built (`--features fdb,etcd`) wyrd
    // image the FDB stack tags `wyrd:fdb`.
    for image in ["foundationdb/foundationdb:", "etcd:", "wyrd:fdb"] {
        assert!(
            merged.contains(image),
            "small-multi-node-fdb is missing the `{image}` image:\n{merged}"
        );
    }
    // Every wyrd role that opens a metadata backend opens `fdb`, and none opens `tikv`.
    // `docker compose config` renders `command` as a normalized YAML/JSON list, so the
    // flag and its value are adjacent tokens; assert on the rendered form too, not only
    // the raw source (the unconditional test above), so a config-time rewrite can't slip a
    // `tikv` role past this gate.
    assert!(
        merged.contains("--metadata-backend"),
        "small-multi-node-fdb must wire the metadata backend on its wyrd roles:\n{merged}"
    );
    assert!(
        !merged.contains("\"tikv\"") && !merged.contains("- tikv"),
        "no rendered role in small-multi-node-fdb may select the `tikv` metadata \
         backend:\n{merged}"
    );
}
