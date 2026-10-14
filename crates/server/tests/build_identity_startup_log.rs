//! The `wyrd` binary records which checkout it was built from (#778): every `wyrd s3`
//! process logs its build identity as the `version` field of its `role started` event.
//!
//! Drives the BUILT binary (`CARGO_BIN_EXE_wyrd`, the `cli_roundtrip.rs` idiom) as a real
//! `s3` role and observes only its stderr — no crate symbol is named, so this file compiles
//! against any tree, and on a tree whose binary records no version it fails on the missing
//! field. The provenance leg derives its expectation INDEPENDENTLY, with its own `git`
//! calls against the workspace's own repository, so the value is bound to the checkout
//! rather than to a constant.

#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const WYRD: &str = env!("CARGO_BIN_EXE_wyrd");

/// The `WYRD_VERSION` this test was compiled under — and so the one the `wyrd` binary
/// cargo built beside it, in the same invocation, was compiled under. Non-empty means
/// that build was TOLD its identity (how `cargo xtask dist` hands in the version it
/// writes to the tarball's `VERSION`), which the binary must then carry verbatim.
const BUILD_OVERRIDE: Option<&str> = option_env!("WYRD_VERSION");

/// How long the role gets to bind and log its startup event. Generous for a loaded CI box;
/// the child is killed on every exit path, so a hang can never outlive the test.
const STARTUP_BUDGET: Duration = Duration::from_secs(60);

/// Kills and reaps the role on drop — the `s3` role serves forever, so EVERY exit path
/// (assertion failure, timeout, panic) must stop it.
struct RoleGuard(Child);

impl Drop for RoleGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Write to the process's real stderr. libtest captures `eprintln!` and throws it away
/// when a test passes, and a leg that is skipped or replaced must be visible on a green
/// run too — so this bypasses the capture.
fn note(message: &str) {
    let _ = writeln!(std::io::stderr(), "build_identity_startup_log: {message}");
}

/// The workspace root (`crates/server` → `../..`), canonicalized so it compares against
/// what git reports.
fn workspace_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/server is nested two levels under the workspace root");
    root.canonicalize().unwrap_or_else(|_| root.to_path_buf())
}

/// Trimmed stdout of `git <args>` against the workspace's OWN repository, or why it
/// failed. Run at the root with every inherited repository-selection override removed
/// (`GIT_DIR`, … — set, for one, under a git hook) and discovery stopped at the root's
/// parent, so neither an environment override nor an enclosing checkout can answer for
/// this workspace.
fn git(args: &[&str]) -> Result<String, String> {
    let root = workspace_root();
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(&root);
    for var in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    ] {
        cmd.env_remove(var);
    }
    if let Some(parent) = root.parent() {
        cmd.env("GIT_CEILING_DIRECTORIES", parent);
    }
    let out = cmd
        .output()
        .map_err(|e| format!("could not run git: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "`git {}` exited {}: {}",
            args.join(" "),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// `HEAD`'s short sha in the workspace's own repository — the repository whose work tree
/// IS the workspace root — or why there is none.
fn own_head_short_sha() -> Result<String, String> {
    let root = workspace_root();
    let toplevel = git(&["rev-parse", "--show-toplevel"])?;
    let toplevel = Path::new(&toplevel)
        .canonicalize()
        .map_err(|e| format!("git reported toplevel `{toplevel}`, which does not resolve: {e}"))?;
    if toplevel != root {
        return Err(format!(
            "git's toplevel is {}, not the workspace root {}",
            toplevel.display(),
            root.display()
        ));
    }
    git(&["rev-parse", "--short", "HEAD"])
}

/// Spawn `wyrd s3` on an ephemeral port, read its stderr until both the serving line and
/// the `role started` JSON event have appeared, and return that event.
fn startup_event() -> serde_json::Value {
    let data_dir = tempfile::tempdir().expect("temp data dir");
    let child = Command::new(WYRD)
        .args([
            "s3",
            "--s3-listen",
            "127.0.0.1:0",
            "--data-dir",
            data_dir.path().to_str().expect("utf-8 temp path"),
            "--access-key",
            "build-identity-test",
            "--secret-key",
            "build-identity-test-secret",
            "--log-format",
            "json",
        ])
        // The role must start on its default local backends at the default level, whatever
        // the calling environment configures.
        .env_remove("WYRD_S3_ACCESS_KEY")
        .env_remove("WYRD_S3_SECRET_KEY")
        .env_remove("WYRD_METADATA_BACKEND")
        .env_remove("WYRD_COORDINATION_BACKEND")
        .env_remove("RUST_LOG")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the wyrd binary");
    let mut guard = RoleGuard(child);
    let stderr = guard.0.stderr.take().expect("piped stderr");

    // A reader thread forwards lines, so the wait below is bounded by a deadline rather
    // than blocked on a read that might never return.
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + STARTUP_BUDGET;
    let mut seen = Vec::new();
    let mut serving = false;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = match rx.recv_timeout(left) {
            Ok(line) => line,
            Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                "wyrd s3 did not log its startup within {STARTUP_BUDGET:?}; stderr so far:\n{}",
                seen.join("\n")
            ),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                "wyrd s3 closed stderr (exited: {:?}) before logging its startup; stderr:\n{}",
                guard.0.try_wait(),
                seen.join("\n")
            ),
        };
        // The listener's real address is reported here, so the ephemeral port is
        // observed, never guessed.
        if line.contains("wyrd s3: serving S3-compatible HTTP on ") {
            serving = true;
        }
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
            let fields = &event["fields"];
            if fields["message"] == "role started" && fields["role"] == "s3" {
                assert!(
                    serving,
                    "the role started event preceded the serving line; stderr:\n{}",
                    seen.join("\n")
                );
                return event;
            }
        }
        seen.push(line);
    }
    // `guard` drops here on every path: the role is killed and reaped.
}

#[test]
fn the_s3_role_logs_the_build_identity_derived_from_git() {
    let event = startup_event();
    let fields = &event["fields"];

    // Leg 1: the identity is recorded, and is not a placeholder.
    let version = fields["version"].as_str().unwrap_or_else(|| {
        panic!("the `role started` event carries no string `version` field: {event}")
    });
    assert!(!version.is_empty(), "empty version: {event}");
    assert_ne!(version, "unknown", "placeholder version: {event}");
    assert_ne!(
        version, "0.0.0",
        "the bare workspace placeholder is not a build identity: {event}"
    );

    // A build that was TOLD its identity carries exactly that — the hand-off `dist` relies
    // on — and derived nothing from git, so the git leg below does not describe it.
    if let Some(told) = BUILD_OVERRIDE.filter(|v| !v.is_empty()) {
        note(&format!(
            "WYRD_VERSION=`{told}` was set for this build, so the binary must carry it \
             verbatim; the git-derivation leg applies only to a build that derived its own"
        ));
        assert_eq!(
            version, told,
            "the build was given WYRD_VERSION=`{told}`, but the binary recorded `{version}`"
        );
        return;
    }

    // Leg 2: it came from git. Probed independently; a probe that SUCCEEDS binds the
    // value, and only a probe that cannot see the workspace's own repository may skip.
    let short_sha = match own_head_short_sha() {
        Ok(sha) => sha,
        Err(why) => {
            note(&format!(
                "SKIP provenance leg: the workspace's own git repository is not visible \
                 ({why})"
            ));
            return;
        }
    };
    // `--always` prints at least the sha whenever HEAD resolves, so a failure here, with
    // HEAD visible, is a broken repository — a failure, not a skip.
    let describe = git(&["describe", "--tags", "--always"])
        .unwrap_or_else(|why| panic!("git sees HEAD at {short_sha}, but {why}"));
    assert!(
        !version.contains("unknown"),
        "git sees HEAD at {short_sha} (describe `{describe}`), yet the binary recorded \
         `{version}` — the identity did not come from the repository"
    );
    match git(&["describe", "--tags", "--exact-match", "HEAD"]) {
        // Not exactly on a tag: describe printed a bare sha or `<tag>-<n>-g<sha>`, and
        // either way the identity must name the sha. (An exact-match probe that fails for
        // any other reason lands here too — it can only make the check stricter.)
        Err(_) => assert!(
            version.contains(&short_sha),
            "HEAD is {short_sha} (describe `{describe}`), but the binary recorded `{version}`"
        ),
        // Exactly on a tag: describe prints the tag and no sha. The identity is that tag's
        // version (`v1.2.3` → `1.2.3`; a tag without the `v` stays as named) or — for a
        // tag that cannot make a usable identity — the commit's sha form. Never an
        // unrelated value.
        Ok(tag) => {
            let tag_version = tag.strip_prefix('v').unwrap_or(&tag);
            assert!(
                (!tag_version.is_empty() && version.ends_with(tag_version))
                    || version.contains(&short_sha),
                "HEAD is {short_sha}, exactly on tag `{tag}`, but the binary recorded \
                 `{version}`"
            );
        }
    }
}
