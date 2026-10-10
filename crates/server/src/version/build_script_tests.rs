//! Real-build regressions for `crates/server/build.rs` (#778): WHEN the build script
//! re-runs, checked with real cargo builds rather than by reading its watch list, and what
//! it does with raw inputs that are not UTF-8 (a `WYRD_VERSION` value, a tag name), which
//! the pure resolver's `&str` inputs cannot express.
//!
//! Each test builds a throwaway crate whose build script IS `crates/server/build.rs`
//! (`build = "<absolute path>"` in its manifest), sitting at `crates/probe` of a
//! throwaway workspace root — the layout the script expects. The test then changes the
//! repository the way a developer would and builds again. A list assertion cannot catch
//! what these pin: cargo deciding the script is fresh after a change that moved the
//! answer (a stale identity), or stale after one that did not (a relink on every edit).
//!
//! They spawn `git` and the cargo running this test (`$CARGO`), offline: the probe crate
//! has no dependencies. Git runs with the user's global and system config shut out, so a
//! local `init.defaultRefFormat` or signing setting cannot change the fixture.

use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// The build script under test.
const BUILD_SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/build.rs");

/// Upper bound on one `git` or `cargo` step; the step is killed past it. Measured on the
/// monotonic clock (`Instant`), which owns nothing but this per-step deadline.
const STEP_BUDGET: Duration = Duration::from_secs(300);

/// One throwaway workspace: `<tmp>/workspace` (the root the build script resolves from
/// `crates/probe`), a separate target dir, and a scratch dir for step output.
struct Fixture {
    _tmp: tempfile::TempDir,
    base: PathBuf,
    root: PathBuf,
    target: PathBuf,
    logs: PathBuf,
}

/// What one successful `cargo build` of the probe produced.
struct Built {
    /// The identity the probe binary carries (`env!("WYRD_BUILD_IDENTITY")`).
    identity: String,
    /// Cargo reused the binary: the build script did not re-run.
    fresh: bool,
    /// Cargo's stderr, where the build script's `cargo:warning=` lines land.
    stderr: String,
}

/// What one finished step printed.
struct Step {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: String,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("temp dir");
        let base = tmp.path().canonicalize().expect("canonical temp dir");
        let fixture = Self {
            root: base.join("workspace"),
            target: base.join("target"),
            logs: base.join("logs"),
            base,
            _tmp: tmp,
        };
        fs::create_dir_all(&fixture.logs).expect("logs dir");
        fixture
    }

    /// Write the probe crate into the workspace root (which may already be a clone).
    fn write_probe(&self) {
        let dir = self.root.join("crates/probe");
        fs::create_dir_all(dir.join("src")).expect("probe dir");
        fs::write(
            dir.join("Cargo.toml"),
            format!(
                "[package]\nname = \"probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
                 publish = false\nbuild = '{BUILD_SCRIPT}'\n\n[workspace]\n"
            ),
        )
        .expect("probe manifest");
        fs::write(
            dir.join("src/main.rs"),
            "fn main() {\n    print!(\"{}\", env!(\"WYRD_BUILD_IDENTITY\"));\n}\n",
        )
        .expect("probe main");
    }

    /// Run `cmd` to completion within [`STEP_BUDGET`], killing it past that, and return
    /// what it printed whatever its exit status. Output goes through files, so a chatty
    /// child can never block on a full pipe.
    fn step(&self, cmd: &mut Command, what: &str) -> Step {
        let out_path = self.logs.join("step.out");
        let err_path = self.logs.join("step.err");
        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(fs::File::create(&out_path).expect("stdout file"))
            .stderr(fs::File::create(&err_path).expect("stderr file"))
            .spawn()
            .unwrap_or_else(|e| panic!("{what}: cannot spawn: {e}"));
        let deadline = Instant::now() + STEP_BUDGET;
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll child") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{what}: still running after {STEP_BUDGET:?}; killed");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        Step {
            status,
            stdout: fs::read(&out_path).unwrap_or_default(),
            stderr: String::from_utf8_lossy(&fs::read(&err_path).unwrap_or_default()).into_owned(),
        }
    }

    /// [`Self::step`], panicking with its stderr unless it succeeds; returns its stdout.
    fn run(&self, cmd: &mut Command, what: &str) -> String {
        let step = self.step(cmd, what);
        assert!(
            step.status.success(),
            "{what} failed ({}):\n{}",
            step.status,
            step.stderr
        );
        String::from_utf8(step.stdout).unwrap_or_else(|e| panic!("{what}: stdout: {e}"))
    }

    /// `git <args>` in `dir`, hermetic: no inherited repository override, no user or
    /// system config. Returns trimmed stdout.
    fn git_in<S: AsRef<OsStr>>(&self, dir: &Path, args: &[S]) -> String {
        let mut cmd = Command::new("git");
        cmd.args(args).current_dir(dir);
        hermetic_git_env(&mut cmd);
        let what = args
            .iter()
            .map(|a| a.as_ref().to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        self.run(&mut cmd, &format!("git {what}"))
            .trim()
            .to_string()
    }

    /// The raw stdout of `git <args>` in the workspace (hermetic, must succeed): for an
    /// answer that need not be UTF-8.
    fn git_bytes(&self, args: &[&str]) -> Vec<u8> {
        let mut cmd = Command::new("git");
        cmd.args(args).current_dir(&self.root);
        hermetic_git_env(&mut cmd);
        let what = format!("git {}", args.join(" "));
        let step = self.step(&mut cmd, &what);
        assert!(step.status.success(), "{what} failed:\n{}", step.stderr);
        step.stdout
    }

    /// A repository at the workspace root with one commit; returns its short sha.
    fn workspace_with_one_commit(&self) -> String {
        fs::create_dir_all(&self.root).expect("workspace dir");
        self.git(&["init", "-q"]);
        fs::write(self.root.join("README"), "probe\n").expect("README");
        self.git(&["add", "README"]);
        self.commit(&self.root, "one");
        self.git(&["rev-parse", "--short", "HEAD"])
    }

    fn git(&self, args: &[&str]) -> String {
        self.git_in(&self.root, args)
    }

    /// Commit in `dir` with a fixed identity and no hooks or signing.
    fn commit(&self, dir: &Path, message: &str) {
        self.git_in(
            dir,
            &[
                "-c",
                "user.name=probe",
                "-c",
                "user.email=probe@example.invalid",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                message,
            ],
        );
    }

    /// A repository at `<base>/origin`: two commits, the lightweight tag `v1.2.3` on the
    /// second, and one commit past it.
    fn origin_one_commit_past_a_tag(&self) -> PathBuf {
        let origin = self.base.join("origin");
        fs::create_dir_all(&origin).expect("origin dir");
        self.git_in(&origin, &["init", "-q"]);
        fs::write(origin.join("README"), "probe\n").expect("README");
        self.git_in(&origin, &["add", "README"]);
        self.commit(&origin, "one");
        self.commit(&origin, "two");
        self.git_in(&origin, &["tag", "v1.2.3"]);
        self.commit(&origin, "three");
        origin
    }

    /// `cargo build` the probe with `WYRD_VERSION` unset and run the binary it produced.
    fn build(&self) -> Built {
        self.try_build(None)
            .unwrap_or_else(|stderr| panic!("cargo build (probe) failed:\n{stderr}"))
    }

    /// `cargo build` the probe with `WYRD_VERSION` set to `wyrd_version` (raw bytes, so a
    /// value that is not UTF-8 can be handed in) or unset, and run the binary it produced.
    /// A failed build is `Err(cargo's stderr)`.
    fn try_build(&self, wyrd_version: Option<&OsStr>) -> Result<Built, String> {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut cmd = Command::new(cargo);
        cmd.args([
            "build",
            "--offline",
            "--message-format=json",
            "--manifest-path",
        ])
        .arg(self.root.join("crates/probe/Cargo.toml"))
        .current_dir(&self.base)
        .env("CARGO_TARGET_DIR", &self.target);
        // A build that is TOLD its identity never consults git (rung 1); none inherits one.
        match wyrd_version {
            Some(value) => cmd.env("WYRD_VERSION", value),
            None => cmd.env_remove("WYRD_VERSION"),
        };
        // The probe is about cargo's freshness decisions, not about how it is compiled:
        // shed any flags or wrappers a coverage or caching run put in the environment.
        for var in [
            "RUSTFLAGS",
            "CARGO_ENCODED_RUSTFLAGS",
            "CARGO_BUILD_RUSTFLAGS",
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
            "LLVM_PROFILE_FILE",
        ] {
            cmd.env_remove(var);
        }
        hermetic_git_env(&mut cmd);
        let step = self.step(&mut cmd, "cargo build (probe)");
        if !step.status.success() {
            return Err(step.stderr);
        }
        let stdout = String::from_utf8(step.stdout).expect("cargo's JSON messages are UTF-8");

        let artifact = stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|msg| {
                msg["reason"] == "compiler-artifact"
                    && msg["target"]["name"] == "probe"
                    && msg["target"]["kind"][0] == "bin"
            })
            .unwrap_or_else(|| panic!("cargo reported no probe binary:\n{stdout}"));
        let fresh = artifact["fresh"].as_bool().expect("`fresh` flag");
        let exe = artifact["executable"]
            .as_str()
            .expect("probe executable path");
        let identity = self.run(&mut Command::new(exe), "probe binary");
        Ok(Built {
            identity,
            fresh,
            stderr: step.stderr,
        })
    }
}

/// Shut out repository overrides and the user's and system's git config, for git run
/// directly and for the git the build script runs under cargo.
fn hermetic_git_env(cmd: &mut Command) {
    for var in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
        "GIT_CEILING_DIRECTORIES",
    ] {
        cmd.env_remove(var);
    }
    cmd.env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
}

/// Let the filesystem clock move past the last build before changing the repository.
/// Cargo treats a watched path whose mtime EQUALS its reference stamp as unchanged, so a
/// change landing in the same timestamp tick would be missed on a coarse filesystem.
fn next_mtime_tick() {
    std::thread::sleep(Duration::from_millis(1100));
}

/// `git fetch --depth=1` on a full clone creates `shallow` and moves no ref, so before the
/// appearance watch the build script never re-ran and kept the tag-derived identity that
/// `git describe` no longer produced. Unshallowing deletes the file and must refresh it
/// back. Neither transition may leave a no-op rebuild re-running the script.
#[test]
fn a_full_clone_made_shallow_and_back_rebuilds_with_each_new_identity() {
    let fx = Fixture::new();
    let origin = fx.origin_one_commit_past_a_tag();
    fx.git_in(
        &fx.base,
        &[
            "clone",
            "-q",
            "--no-local",
            origin.to_str().expect("utf-8 path"),
            "workspace",
        ],
    );
    fx.write_probe();
    let sha = fx.git(&["rev-parse", "--short", "HEAD"]);
    let tagged = format!("1.2.3+git.1.{sha}");
    let shallow_form = format!("0.0.0+git.{sha}");

    let first = fx.build();
    assert_eq!(first.identity, tagged, "full clone, one commit past v1.2.3");
    let again = fx.build();
    assert!(again.fresh, "a no-op rebuild re-ran the build script");
    assert_eq!(again.identity, tagged);

    next_mtime_tick();
    let refs_before = fx.git(&["for-each-ref"]);
    fx.git(&["fetch", "-q", "--depth=1", "--no-tags", "origin", "HEAD"]);
    // The fixture must really be the hard case: the history is cut, no ref moved.
    assert!(
        fx.root.join(".git/shallow").is_file(),
        "fixture: the fetch did not make the clone shallow"
    );
    assert_eq!(
        fx.git(&["for-each-ref"]),
        refs_before,
        "fixture: a ref moved"
    );
    assert_eq!(
        fx.git(&["describe", "--tags", "--always"]),
        sha,
        "fixture: the shallow boundary did not hide v1.2.3"
    );

    let shallow = fx.build();
    assert_eq!(
        shallow.identity, shallow_form,
        "the clone became shallow and `git describe` changed, but the build kept the old identity"
    );
    assert!(!shallow.fresh);
    assert!(
        fx.build().fresh,
        "once `shallow` exists, a no-op rebuild must not re-run the build script"
    );

    next_mtime_tick();
    fx.git(&["fetch", "-q", "--unshallow", "--no-tags", "origin", "HEAD"]);
    assert!(
        !fx.root.join(".git/shallow").exists(),
        "fixture: still shallow"
    );
    let full = fx.build();
    assert_eq!(
        full.identity, tagged,
        "the clone was unshallowed, but the build kept the shallow identity"
    );
    assert!(
        fx.build().fresh,
        "after unshallowing, a no-op rebuild must not re-run the build script"
    );
}

/// Decision 1: the identity names a commit, so an edit, a `git status` and a `git add` —
/// the last two rewrite the index — must not re-run the build script and relink. A
/// commit must.
#[test]
fn an_edit_and_the_index_do_not_rerun_the_build_script_but_a_commit_does() {
    let fx = Fixture::new();
    let first_sha = fx.workspace_with_one_commit();
    fx.write_probe();
    assert_eq!(fx.build().identity, format!("0.0.0+git.{first_sha}"));

    next_mtime_tick();
    let index = fx.root.join(".git/index");
    let index_before = fs::metadata(&index)
        .and_then(|m| m.modified())
        .expect("index mtime");
    fs::write(fx.root.join("README"), "probe, edited\n").expect("edit");
    fx.git(&["status", "--porcelain"]);
    fx.git(&["add", "README"]);
    let index_after = fs::metadata(&index)
        .and_then(|m| m.modified())
        .expect("index mtime");
    assert!(
        index_after > index_before,
        "fixture: the index was not rewritten"
    );
    let edited = fx.build();
    assert!(
        edited.fresh,
        "an edit and an index rewrite re-ran the build script"
    );
    assert_eq!(edited.identity, format!("0.0.0+git.{first_sha}"));

    next_mtime_tick();
    fx.commit(&fx.root, "two");
    let second_sha = fx.git(&["rev-parse", "--short", "HEAD"]);
    let committed = fx.build();
    assert_eq!(
        committed.identity,
        format!("0.0.0+git.{second_sha}"),
        "HEAD moved, but the build kept the old identity"
    );
}

/// A build that found no repository (rung 3) must ask again on the next build: before
/// the re-probe, `git init` and a first commit left `0.0.0+git.unknown` baked in until a
/// `cargo clean`. Once git answers, the re-probing stops.
#[test]
fn a_build_with_no_repository_reprobes_until_one_exists() {
    let fx = Fixture::new();
    fx.write_probe();

    let unknown = fx.build();
    assert_eq!(unknown.identity, "0.0.0+git.unknown");
    let reprobed = fx.build();
    assert!(
        !reprobed.fresh,
        "a build that could not name its commit must re-run the build script"
    );
    assert_eq!(reprobed.identity, "0.0.0+git.unknown");

    next_mtime_tick();
    fx.git(&["init", "-q"]);
    fx.commit(&fx.root, "one");
    let sha = fx.git(&["rev-parse", "--short", "HEAD"]);
    assert_eq!(
        fx.build().identity,
        format!("0.0.0+git.{sha}"),
        "a repository appeared, but the build kept `unknown`"
    );
    assert!(
        fx.build().fresh,
        "once the commit is known, a no-op rebuild must not re-run the build script"
    );
}

/// Git accepts a tag name holding bytes that are not UTF-8, and `git describe` prints them
/// raw. The build script used to drop that output as absent and fall to rung 3,
/// `0.0.0+git.unknown`, although the commit's sha was readable. It must fail closed to the
/// sha form, with a warning that says why.
#[test]
fn an_undecodable_tag_still_names_the_commit_by_its_sha() {
    let fx = Fixture::new();
    let sha = fx.workspace_with_one_commit();
    fx.git_in(
        &fx.root,
        &[OsStr::new("tag"), OsStr::from_bytes(b"v1.0\xff")],
    );
    fx.write_probe();
    // The fixture must really be the fault: describe answers, and not in UTF-8.
    let describe = fx.git_bytes(&["describe", "--tags", "--always"]);
    assert!(
        std::str::from_utf8(&describe).is_err(),
        "fixture: `git describe` printed UTF-8: {describe:?}"
    );

    let built = fx.build();
    assert_eq!(
        built.identity,
        format!("0.0.0+git.{sha}"),
        "a tag that is not UTF-8 must not discard the readable sha"
    );
    // The warning quotes what git printed: the output was decoded and refused, not
    // dropped as "no answer" (which the resolver would also turn into the sha form).
    assert!(
        built.stderr.contains("`git describe` printed `v1.0")
            && built.stderr.contains("sha-derived"),
        "the fail-closed build must say why in its build log:\n{}",
        built.stderr
    );
}

/// Rung 1 at the environment-reading boundary, in a repository where rung 2 WOULD answer,
/// so a value read as unset shows up as the git identity. A valid override is baked
/// verbatim; one that does not validate fails the build; and one that is not UTF-8 fails
/// it too, where reading it with `std::env::var(..).ok()` treated it as unset and baked
/// `0.0.0+git.<sha>` in its place. The first build is fresh, as every image build is.
#[test]
fn an_override_is_baked_verbatim_and_one_that_is_not_utf8_fails_the_build() {
    let fx = Fixture::new();
    let sha = fx.workspace_with_one_commit();
    fx.write_probe();
    let refuses = |value: &OsStr, why: &str| match fx.try_build(Some(value)) {
        Ok(built) => panic!(
            "WYRD_VERSION={value:?} must fail the build, but it baked `{}`",
            built.identity
        ),
        Err(stderr) => assert!(
            stderr.contains("WYRD_VERSION is not usable") && stderr.contains(why),
            "WYRD_VERSION={value:?}: the build failed for another reason:\n{stderr}"
        ),
    };
    let not_utf8 = OsStr::from_bytes(b"1.2.3\xff");

    refuses(not_utf8, "not valid UTF-8");

    let dirty_release = "0.1.0+git.3.abc12de.dirty";
    let told = fx
        .try_build(Some(OsStr::new(dirty_release)))
        .unwrap_or_else(|stderr| panic!("a valid override failed the build:\n{stderr}"));
    assert_eq!(told.identity, dirty_release, "rung 1 is used verbatim");

    // Incremental, coming from a set value: cargo re-runs the script, which refuses.
    refuses(not_utf8, "not valid UTF-8");
    refuses(OsStr::new("bad value"), "Docker tag");

    assert_eq!(
        fx.build().identity,
        format!("0.0.0+git.{sha}"),
        "with the override unset, rung 2 answers"
    );
}
