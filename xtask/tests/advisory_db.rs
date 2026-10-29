//! #986 / ADR-0049: the dependency wall audits a PINNED RustSec advisory database.
//!
//! Before this, every `cargo deny` run fetched the newest database, so an advisory published
//! for a crate already on `main` turned the `gate` check of every open PR red at once (#979,
//! RUSTSEC-2026-0414). These tests pin the parts the fix depends on: the gitlink that records
//! the commit, the `db-path` both deny configs share, `--offline` on every wall invocation,
//! and the two places that still audit the newest database (release and the daily run).

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use xtask::{
    advisory_invocations, dependency_wall_invocations, offline, parse_gitlink, ADVISORY_DB_PATH,
    ADVISORY_DB_SUBMODULE,
};

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

fn git(args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(workspace_root())
        .output()
        .expect("spawn git");
    assert!(output.status.success(), "git {args:?} failed: {output:?}");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

const SHA: &str = "40f5be6fb12ed93fc591bf4ab23b7d793b3f2239";

#[test]
fn parse_gitlink_reads_the_pinned_commit() {
    let ls = format!("160000 {SHA} 0\t{ADVISORY_DB_SUBMODULE}\n");
    assert_eq!(parse_gitlink(&ls), Ok(SHA.to_string()));
}

#[test]
fn parse_gitlink_refuses_anything_but_a_gitlink() {
    // No entry, a regular file at the path, a truncated hash: each must be an error, never a
    // silent fall-back to an unpinned database.
    for ls in [
        String::new(),
        format!("100644 {SHA} 0\t{ADVISORY_DB_SUBMODULE}\n"),
        format!("160000 {} 0\t{ADVISORY_DB_SUBMODULE}\n", &SHA[..12]),
    ] {
        assert!(parse_gitlink(&ls).is_err(), "accepted {ls:?}");
    }
}

#[test]
fn the_index_pins_the_rustsec_database_as_a_declared_submodule() {
    let ls = git(&["ls-files", "-s", "--", ADVISORY_DB_SUBMODULE]);
    parse_gitlink(&ls).expect("the advisory-db pin is a gitlink in the index");

    let url = git(&[
        "config",
        "-f",
        ".gitmodules",
        "--get",
        "submodule.advisory-db.url",
    ]);
    assert_eq!(url.trim(), "https://github.com/RustSec/advisory-db");
    let path = git(&[
        "config",
        "-f",
        ".gitmodules",
        "--get",
        "submodule.advisory-db.path",
    ]);
    assert_eq!(path.trim(), ADVISORY_DB_SUBMODULE);
}

#[test]
fn both_deny_configs_keep_the_database_where_xtask_pins_it() {
    // A config without the setting would make cargo-deny read the shared
    // $CARGO_HOME/advisory-dbs copy, which xtask never moves to the pin.
    let setting = format!("db-path = \"{ADVISORY_DB_PATH}\"");
    for config in ["deny.toml", "deny-all-features.toml"] {
        assert!(
            read(config).lines().any(|l| l.trim() == setting),
            "{config} must set `{setting}` under [advisories]"
        );
    }
}

#[test]
fn offline_puts_the_flag_before_the_subcommand() {
    for wall in dependency_wall_invocations() {
        let args = offline(&wall);
        assert_eq!(&args[..2], ["deny", "--offline"], "{args:?}");
        assert_eq!(&args[2..], &wall[1..], "{args:?}");
    }
}

#[test]
fn the_advisories_task_audits_the_same_graphs_as_the_wall() {
    // The default graph from deny.toml, and the off-by-default trees from
    // deny-all-features.toml with its quarantined ignores (#543): `--latest` at release must
    // not judge either graph by a different policy than the PR gate.
    let advisories = advisory_invocations();
    let walls = dependency_wall_invocations();
    assert_eq!(advisories[0], ["deny", "check", "advisories"]);
    assert_eq!(
        walls[0],
        ["deny", "check"],
        "the default wall runs every check"
    );
    assert!(
        walls.contains(&advisories[1]),
        "the all-features advisory run must be the wall's own: {:?}",
        advisories[1]
    );
    assert_eq!(advisories.len(), 2);
}

#[test]
fn release_and_the_daily_run_audit_the_newest_database() {
    let release = read(".github/workflows/release.yml");
    assert!(
        release.contains("run: cargo xtask advisories --latest"),
        "release.yml must audit the newest database before building"
    );
    assert!(
        release.contains("  release:\n    needs: advisories\n"),
        "the release job must wait for the advisory check"
    );

    let daily = read(".github/workflows/advisories.yml");
    assert!(daily.contains("run: cargo xtask advisories --latest"));
    assert!(
        daily.contains("schedule:"),
        "advisories.yml must run on a schedule"
    );

    let dependabot = read(".github/dependabot.yml");
    assert!(
        dependabot.contains("package-ecosystem: gitsubmodule"),
        "Dependabot must move the advisory-db pin"
    );
}
