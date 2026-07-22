//! Server-side FDB metadata-backend selection (issue #440, ADR-0042 "selection is
//! not deployment", `docs/design/adr/0042-production-metadata-backend-reevaluation.md:463`).
//! #438 landed the `wyrd-metadata-fdb` driver (PR #492), but no `server`-side
//! selection arm reached it: `--metadata-backend fdb` was rejected as unknown. This
//! is a **NEW file** (not an edit to `backend_selection.rs`) so `C4-verify`'s
//! `_added_files` / `ADDED_TESTS` discrimination sees it as an added test and the
//! RED phase is genuine (see brief "Test file" note).
//!
//! Three binding assertions, all message-content (not `is_err()` — pre-fix
//! `from_config(Some("fdb"))` already returns `Err`, just with the wrong text):
//!
//! 1. `#[cfg(not(feature = "fdb"))]`-gated: the default build rejects `fdb` with a
//!    build-hint mentioning `--features fdb`, mirroring the tikv text (`cli.rs:117`).
//! 2. `from_config(Some("nonsense"))`'s message must itself mention `fdb` — proof the
//!    unknown-value text lists all three backends, not just two.
//! 3. The compiled binary's no-args usage (stderr) lists `redb|tikv|fdb`.

#![forbid(unsafe_code)]
// This patch declares the `fdb` feature (`crates/server/Cargo.toml:31`), so with the fix
// applied `feature = "fdb"` is a known cfg value and this allow is inert. It exists for
// the ONE tree where the feature is not declared: `C4-verify`'s RED phase, which reverts
// every modified file — `crates/server/Cargo.toml` included
// (`engine/scripts/run-verify.sh:264`) — while keeping this added test (`:260`). Without
// the allow, the RED would be a *compile* error (`unexpected_cfgs`, hardened into an error
// by the sibling `warnings = "deny"`, `Cargo.toml:195-196`), proving only "the crate does
// not build"; with it, the RED is three genuine assertion failures on message content,
// which is what the regression contract is actually about. The allow cannot hide a typo'd
// cfg value: each gated test appears by name in exactly one of the two runs recorded in
// build-notes (default build vs `--features fdb`), so a misspelling would show up there as
// a missing test name.
#![allow(unexpected_cfgs)]

use std::process::Command;

use wyrd_server::cli::MetadataBackend;

/// (1) Pre-fix `from_config(Some("fdb"))` was ``Err("unknown metadata backend `fdb`
/// (expected `redb` or `tikv`)")`` — an `Err`, but the WRONG one: no build hint, no
/// `--features fdb` mention. Gated `#[cfg(not(feature = "fdb"))]` because under
/// `--features fdb` (this brief's supplementary, non-Check run) `from_config` legitimately
/// returns `Ok(Fdb)`, which would make this assertion false there (not a regression).
#[cfg(not(feature = "fdb"))]
#[test]
fn fdb_without_the_feature_names_the_build_flag() {
    let err = MetadataBackend::from_config(Some("fdb"))
        .expect_err("fdb must be rejected in a build with no `fdb` feature");
    let message = err.to_string();
    assert!(
        message.contains("requires building `wyrd` with `--features fdb`"),
        "expected the build-hint text (mirroring the tikv hint at cli.rs), got: {message:?}"
    );
}

/// The gated positive counterpart (etcd pattern, `backend_selection.rs:52-56`):
/// under `--features fdb`, `fdb` selects the `Fdb` variant outright. Deferred
/// off-Check per the brief's "Verification posture" (`C4-ci` builds default
/// features only); run directly with `cargo test -p wyrd-server --features fdb
/// --test fdb_backend_selection`.
#[cfg(feature = "fdb")]
#[test]
fn fdb_with_the_feature_selects_the_fdb_backend() {
    assert_eq!(
        MetadataBackend::from_config(Some("fdb")).unwrap(),
        MetadataBackend::Fdb,
    );
}

/// (2) An unrelated unknown value (`nonsense`, deliberately NOT `fdb`, so the probe
/// value itself can never satisfy the substring check) must produce a message that
/// mentions `fdb` — proof the unknown-backend text lists all three names. Pre-fix the
/// message was ``(expected `redb` or `tikv`)``, containing neither `fdb` nor any
/// hint of it.
#[test]
fn unknown_backend_message_lists_fdb_as_a_known_backend() {
    let err = MetadataBackend::from_config(Some("nonsense"))
        .expect_err("an unrecognised backend name must be a config error");
    let message = err.to_string();
    assert!(
        message.contains("fdb"),
        "the unknown-backend message must mention `fdb` among the known backends, got: {message:?}"
    );
}

/// (3) The compiled `wyrd` binary's no-args usage, on stderr, must list `fdb`
/// alongside `redb` and `tikv`. Pre-fix, `usage()` (post-fix `cli.rs:266-272`) printed
/// `redb|tikv` with no `fdb`.
#[test]
fn usage_lists_fdb_as_a_metadata_backend() {
    let out = Command::new(env!("CARGO_BIN_EXE_wyrd"))
        .output()
        .expect("run the wyrd binary with no arguments");
    assert!(
        !out.status.success(),
        "no subcommand is a usage error (exit 2)"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("redb|tikv|fdb"),
        "usage on stderr must list all three metadata backends, got: {stderr:?}"
    );
}
