//! ADR-0010 structural guard: **"no code couples to orchestrator APIs."** The
//! `deploy/` bring-up for the single-zone "Small multi-node Production" tier (M4.5,
//! issue #256; proposal 0015 §"Deployment: TiKV/PD as a stateful, disk-affine,
//! orchestrator-agnostic tier") puts TiKV/PD/etcd/D-servers under `deploy/`, OUTSIDE
//! the Cargo workspace, precisely so the invariant is mechanically checkable: no
//! workspace crate may import a Kubernetes/orchestrator client API. This is a
//! lightweight grep-style guard (the ADR-0016 single-source style, alongside the
//! ADR-0035 `run_statics` gate in `main.rs`) over every `.rs` file under `crates/` —
//! not a full reachability analysis, which the invariant does not need: any import at
//! all of an orchestrator client is the violation.
//!
//! Exposed as a **library** module (unlike `run_statics`, which is inlined in
//! `main.rs`) so `xtask/tests/deploy_no_orchestrator_coupling.rs` can drive the SAME
//! production scan function `main.rs`'s `run_orchestrator_guard` runs as part of
//! `cargo xtask ci` — one scan, two call sites — proving the guard is load-bearing
//! rather than resting red on non-existence (the "demonstrated red" the brief
//! requires): the test plants a real orchestrator import in a temp fixture and shows
//! `scan_dir` catches it, then shows `scan_dir` is clean over the real `crates/` tree.

use std::path::{Path, PathBuf};

/// Substrings that name a Kubernetes / orchestrator client API import (ADR-0010:
/// "no code couples to orchestrator APIs"; architecture §7.2: "peers are discovered
/// through L5, never through orchestrator APIs"). Matched against real Rust import
/// syntax (`kube::`, `use k8s_openapi`, …) so a prose mention — e.g.
/// `deploy/README.md`'s "Kubernetes is available, never required" — is not a false
/// positive: only `.rs` source is scanned (see `scan_dir`), never docs.
pub const ORCHESTRATOR_NEEDLES: &[&str] = &[
    "kube::",
    "use kube::",
    "kube_client::",
    "k8s_openapi::",
    "use k8s_openapi",
    "kube_runtime::",
];

/// Scan one source line for an orchestrator-import needle, returning the matched
/// needle. Comment lines are skipped so a doc comment mentioning Kubernetes prose is
/// not a false positive (mirrors `main.rs`'s `statics_scan_line`). Pure (no IO), so
/// it is unit-tested directly with planted sample lines.
pub fn scan_line(raw: &str) -> Option<&'static str> {
    let line = raw.trim_start();
    if line.starts_with("//") || line.starts_with('*') || line.starts_with("/*") {
        return None;
    }
    ORCHESTRATOR_NEEDLES
        .iter()
        .find(|needle| raw.contains(*needle))
        .copied()
}

/// Recursively collect every `.rs` file under `dir` (order not guaranteed; `scan_dir`
/// sorts before scanning for deterministic output).
pub fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Scan every `.rs` file under `dir` for an orchestrator-import needle, returning one
/// `"<path>:<line>: <needle>"` string per hit (empty ⇒ clean). This is the SAME
/// function `cargo xtask ci` runs over `crates/` (`run_orchestrator_guard` in
/// `main.rs`) and that the flippable regression test drives over a planted fixture —
/// one guard, two call sites.
pub fn scan_dir(dir: &Path) -> Vec<String> {
    let mut files = Vec::new();
    collect_rs_files(dir, &mut files);
    files.sort();
    let mut violations = Vec::new();
    for file in files {
        let Ok(content) = std::fs::read_to_string(&file) else {
            continue;
        };
        for (idx, raw) in content.lines().enumerate() {
            if let Some(needle) = scan_line(raw) {
                violations.push(format!("{}:{}: {needle}", file.display(), idx + 1));
            }
        }
    }
    violations
}
