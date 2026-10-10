//! Bake the build identity into `wyrd-server` as `WYRD_BUILD_IDENTITY` (#778), read back
//! by `wyrd_server::version::BUILD_IDENTITY`.
//!
//! A thin caller: it gathers the inputs (the `WYRD_VERSION` build variable, then
//! `git describe --tags --always` and `git rev-parse --short HEAD` of the workspace's own
//! repository) and hands them to `derivation::resolve`, the pure resolver shared with
//! `cargo xtask dist` (`src/version/derivation.rs`). The rung order, the validator, and
//! the reason there is no `--dirty` are documented there and in `src/version.rs`.
//!
//! Both raw inputs are decoded by the shared module too, and neither decoding turns an
//! answer into "absent": a `WYRD_VERSION` that is not UTF-8 fails the build
//! (`derivation::override_from_env`), like any other value that does not validate, and a
//! `git describe` output that is not UTF-8 (git accepts such bytes in a tag name) fails
//! closed to the sha form (`derivation::describe_text`).
//!
//! One gap cargo leaves, which no build script can close: cargo's own check of
//! `rerun-if-env-changed` reads a value that is not UTF-8 as unset (measured on cargo
//! 1.96.1). So an INCREMENTAL build that goes from `WYRD_VERSION` unset to such a value
//! does not run this script at all and keeps the identity it had. A fresh build (every
//! image build) and one coming from any set value do run it, and fail. `cargo xtask dist`
//! is not exposed: it only ever hands in a value it has validated.
//!
//! ## When this script re-runs
//!
//! The identity names a commit, so the script re-runs when an input to that answer can
//! have moved, and never on an edit or a `git status`. "Can have moved" is coarse: any
//! ref change re-runs it (a new branch, a fetch, a commit in another linked worktree),
//! whether or not it changes `git describe` of this `HEAD`. The inputs:
//!
//! * `WYRD_VERSION` (rung 1);
//! * the repository paths `derivation::watch_candidates` names: `HEAD`, the refs in either
//!   storage format, the shallow boundary. A path that exists is watched directly, which
//!   catches a change or a deletion. A path that does not exist YET is watched for its
//!   appearance. It cannot be watched directly: cargo treats a missing watched path as
//!   always stale, and would re-run this script — and recompile this crate and relink
//!   every integration-test binary — on every build. So the script keeps a symlink to
//!   each absent path in a private directory under `OUT_DIR` and watches that directory.
//!   Cargo's scan of a watched directory follows symlinks and skips a dangling one, so the
//!   directory reads as unchanged until a target appears, and then as changed by the
//!   target's own mtime. That is how a full clone made shallow (`git fetch --depth`
//!   creates `shallow` and moves no ref) re-runs the script. Both halves of that cargo
//!   behaviour are pinned by real incremental builds in `src/version/build_script_tests.rs`,
//!   so a toolchain whose cargo scans differently fails CI instead of going stale.
//!
//! Deliberately NOT watched: the index and the source files. Watching them would re-run
//! this script on every edit or `git status`, to refresh a value that depends on neither.
//!
//! A build that cannot name its commit at all (rung 3) re-runs on EVERY build instead, so
//! its identity recovers as soon as git can answer. Whatever stopped git — no `.git` of
//! the workspace's own, `git` missing from `PATH`, a checkout git refuses to read — can be
//! fixed without touching any path this script could watch. Only builds that already warn
//! pay that cost.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

// The build script uses only `resolve`, `watch_candidates` and their results; the
// module's other public items serve the crate and `cargo xtask dist`.
#[allow(dead_code)]
#[path = "src/version/derivation.rs"]
mod derivation;

fn main() {
    println!("cargo:rerun-if-env-changed=WYRD_VERSION");
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets OUT_DIR"));
    // `var_os`, not `var`: a value that is not UTF-8 must fail the build, not read as unset.
    let raw_override = std::env::var_os("WYRD_VERSION");
    let override_value = derivation::override_from_env(raw_override.as_deref())
        .unwrap_or_else(|e| panic!("wyrd-server build identity: {e}"));

    let (describe, short_sha) = if override_value.is_some_and(|v| !v.is_empty()) {
        // Rung 1 decides; git is not consulted, so no git path is watched.
        (None, None)
    } else {
        match workspace_root().and_then(|root| own_repository(&root).map(|git| (root, git))) {
            Some((root, git)) => {
                watch(
                    &out_dir,
                    &derivation::watch_candidates(&git.git_dir, &git.common_dir),
                );
                (
                    // A tag name may hold bytes that are not UTF-8. The shared decoder
                    // keeps such output as an answer, so it fails closed to the sha form
                    // instead of going missing and taking the readable sha with it.
                    git_stdout(&root, &["describe", "--tags", "--always"])
                        .map(|out| derivation::describe_text(&out).into_owned()),
                    git_output(&root, &["rev-parse", "--short", "HEAD"]),
                )
            }
            None => (None, None),
        }
    };

    let resolved = derivation::resolve(
        override_value,
        describe.as_deref(),
        short_sha.as_deref(),
        derivation::FALLBACK_BASE,
    )
    .unwrap_or_else(|e| panic!("wyrd-server build identity: {e}"));
    for warning in &resolved.warnings {
        println!("cargo:warning=wyrd-server build identity: {warning}");
    }
    if resolved.rung == derivation::Rung::Fallback {
        // Rung 3 re-probes on every build (module docs): a watched path that never
        // exists is stale to cargo on every build.
        let never = out_dir.join("rung-3-reprobe.never-created");
        let _ = std::fs::remove_file(&never);
        println!("cargo:rerun-if-changed={}", never.display());
        println!(
            "cargo:warning=wyrd-server build identity: this build script will run again on \
             every build until it can name the commit"
        );
    }
    println!("cargo:rustc-env=WYRD_BUILD_IDENTITY={}", resolved.identity);
}

/// Watch each repository path rung 2 depends on: an existing one directly, an absent one
/// for its appearance (module docs).
fn watch(out_dir: &Path, candidates: &[PathBuf]) {
    let (present, absent): (Vec<&PathBuf>, Vec<&PathBuf>) =
        candidates.iter().partition(|p| p.exists());
    for path in present {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    let appear_dir = out_dir.join("git-paths-not-yet-present");
    match link_absent_paths(&appear_dir, &absent) {
        Ok(()) => println!("cargo:rerun-if-changed={}", appear_dir.display()),
        Err(e) => {
            // No way to watch for an appearance here, so watch the absent paths
            // themselves: cargo then re-runs the script on every build. Slower, but the
            // identity never goes stale.
            println!(
                "cargo:warning=wyrd-server build identity: cannot link the git paths that do \
                 not exist yet under {} ({e}); this build script will run again on every \
                 build",
                appear_dir.display()
            );
            for path in absent {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
}

/// Make `dir` hold exactly one symlink per path in `absent`, and nothing else.
fn link_absent_paths(dir: &Path, absent: &[&PathBuf]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let wanted: Vec<(String, &Path)> = absent
        .iter()
        .enumerate()
        .map(|(i, target)| (i.to_string(), target.as_path()))
        .collect();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let current = std::fs::read_link(entry.path()).ok();
        let keep = wanted.iter().any(|(want_name, want_target)| {
            name.to_str() == Some(want_name.as_str()) && current.as_deref() == Some(*want_target)
        });
        if !keep {
            std::fs::remove_file(entry.path())?;
        }
    }
    for (name, target) in &wanted {
        let link = dir.join(name);
        if std::fs::symlink_metadata(&link).is_err() {
            symlink(target, &link)?;
        }
    }
    // Creating or removing a link sets the directory's own mtime, which cargo's scan
    // counts. That write happens while this script runs, after cargo's reference
    // timestamp, so left alone it would re-run the script once more on the next build.
    // Best-effort: if it fails, that one extra re-run is the whole cost.
    let _ = std::fs::File::open(dir).and_then(|d| {
        d.set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
    });
    Ok(())
}

#[cfg(unix)]
fn symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

/// Symlinks are made only on unix (every CI runner and release build). Elsewhere the
/// caller falls back to watching the absent paths directly.
#[cfg(not(unix))]
fn symlink(_target: &Path, _link: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "symlinks are only made on unix",
    ))
}

/// The workspace root: `crates/server` → `../..`, canonicalized so it compares against
/// what git reports.
fn workspace_root() -> Option<PathBuf> {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR")?);
    manifest.parent()?.parent()?.canonicalize().ok()
}

/// A `git` invocation scoped to the workspace's OWN repository: run at the root, with
/// discovery stopped at the root's parent (`GIT_CEILING_DIRECTORIES`) and any inherited
/// repository override (`GIT_DIR`, … — set, for one, when cargo runs under a git hook)
/// removed. A workspace with no `.git` of its own (an unpacked source tarball, the image
/// build's context) therefore finds NO repository, rather than an enclosing one.
fn git(root: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(root);
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
    cmd
}

/// Raw stdout of a successful `git` run; `None` if it could not run or failed (git
/// missing, no repository, refused ownership, …), which falls through to the next rung.
fn git_stdout(root: &Path, args: &[&str]) -> Option<Vec<u8>> {
    let out = git(root, args).output().ok()?;
    out.status.success().then_some(out.stdout)
}

/// Trimmed, non-empty stdout of a successful `git` run whose answer is a path or a sha;
/// `None` otherwise. Strict UTF-8 costs nothing for the work tree (cargo refuses to build
/// a workspace at a path that is not UTF-8) or a sha (hex). A git dir kept at such a path
/// outside the work tree reads as no repository: rung 3, warned and re-probed on every
/// build, never a wrong identity.
fn git_output(root: &Path, args: &[&str]) -> Option<String> {
    let text = String::from_utf8(git_stdout(root, args)?).ok()?;
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

/// Where the workspace's own repository keeps its metadata.
struct OwnRepository {
    /// The per-worktree git dir (holds `HEAD`).
    git_dir: PathBuf,
    /// The common dir (holds the refs and `shallow`; equal to `git_dir` outside a linked
    /// worktree).
    common_dir: PathBuf,
}

/// The repository whose work tree IS the workspace root, or `None`.
fn own_repository(root: &Path) -> Option<OwnRepository> {
    let toplevel = git_output(root, &["rev-parse", "--show-toplevel"])?;
    if Path::new(&toplevel).canonicalize().ok()? != root {
        return None;
    }
    let resolve = |p: String| {
        let p = PathBuf::from(p);
        if p.is_absolute() {
            p
        } else {
            root.join(p)
        }
    };
    Some(OwnRepository {
        git_dir: resolve(git_output(root, &["rev-parse", "--git-dir"])?),
        common_dir: resolve(git_output(root, &["rev-parse", "--git-common-dir"])?),
    })
}
