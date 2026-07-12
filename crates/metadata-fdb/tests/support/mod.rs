//! Shared plumbing for the FDB Tier-1 fault legs (#442): who to cut, who to ask, how to ask.
//!
//! Deliberately thin. Everything that *decides* pass or fail lives in the shared
//! `wyrd-metadata-fault-conformance` scenario or in `wyrd-testkit`'s unit-checked oracles; this module only
//! moves bytes — it resolves the cluster's topology from the runner-exported netns map and
//! shells out to `docker`.

#![allow(dead_code)] // each test binary uses a subset

use std::time::Duration;

/// One FDB process: its static bridge IP, its `<ip>:<port>` address as `status json` names it,
/// and the container that owns its network namespace.
#[derive(Debug, Clone)]
pub struct Process {
    pub ip: String,
    pub addr: String,
    pub container: String,
}

/// The cluster's processes, resolved from `WYRD_TIER1_NETNS_MAP` (`ip=container,…` — the same
/// map the TiKV leg uses), or `None` when the runner configured no fault.
///
/// The port is fixed by `deploy/fdb-multi-replica` (all three processes listen on 4500), and
/// is overridable via `WYRD_TIER1_FDB_PORT` so the map stays a pure ip→container mapping.
pub fn processes() -> Option<Vec<Process>> {
    let map = std::env::var("WYRD_TIER1_NETNS_MAP").ok()?;
    let port = std::env::var("WYRD_TIER1_FDB_PORT").unwrap_or_else(|_| "4500".to_string());
    let processes: Vec<Process> = map
        .split(',')
        .filter_map(|pair| {
            let (ip, container) = pair.trim().split_once('=')?;
            let (ip, container) = (ip.trim(), container.trim());
            (!ip.is_empty() && !container.is_empty()).then(|| Process {
                ip: ip.to_string(),
                addr: format!("{ip}:{port}"),
                container: container.to_string(),
            })
        })
        .collect();
    (!processes.is_empty()).then_some(processes)
}

/// `fdbcli --exec "status json"`, run **inside `container`**.
///
/// Always a *peer's* view. Which peer matters: the fault-effect oracle must never ask the node
/// it just cut whether it is cut (a node's own view of its isolation proves nothing about
/// whether the cluster noticed — Invariant B).
pub fn status_json(container: &str) -> Option<String> {
    let out = std::process::Command::new("docker")
        .args([
            "exec",
            container,
            "fdbcli",
            "--timeout",
            "10",
            "--exec",
            "status json",
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Poll any reachable process until the cluster names the holder of `role`, returning that
/// process. `None` = unresolvable within `timeout`, and the caller must then refuse to cut a
/// guess rather than pick a node and call it the master.
///
/// Asking *any* process is sound here — `status json` is a cluster-wide report, not a local
/// one — and it is what breaks the chicken-and-egg: we cannot pick a survivor to ask until we
/// know the target, and we cannot know the target until we ask.
pub fn resolve_role_holder(all: &[Process], role: &str, timeout: Duration) -> Option<Process> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        for p in all {
            if let Some(status) = status_json(&p.container) {
                if let Some(addr) = wyrd_testkit::parse_fdb_process_with_role(&status, role) {
                    if let Some(holder) = all.iter().find(|c| c.addr == addr) {
                        return Some(holder.clone());
                    }
                    eprintln!(
                        "wyrd-tier1-fdb: the {role} is at {addr}, which is not in the netns map \
                         — refusing to cut a node the runner did not declare."
                    );
                    return None;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// A process that is **not** `target` — the one the oracle interrogates.
pub fn survivor<'a>(all: &'a [Process], target: &Process) -> Option<&'a Process> {
    all.iter().find(|p| p.container != target.container)
}

/// Run `docker <args>`; a non-zero exit is an `Err`, never swallowed.
pub fn docker(args: &[&str]) -> Result<(), String> {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("spawn docker: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "docker {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim(),
        ))
    }
}
