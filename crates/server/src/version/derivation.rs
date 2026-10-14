//! The build-identity derivation (#778): ONE definition, compiled by THREE consumers.
//!
//! * `crates/server/build.rs` — bakes the identity into the `wyrd` binary
//!   (`#[path]`-included there, so the build script has no dependency on this crate);
//! * `wyrd_server::version` — re-exports it so its unit tests run in the product crate;
//! * `xtask/src/dist.rs` — `cargo xtask dist` names its artifacts with
//!   [`normalize_describe`] (`#[path]`-included, so the dependency runs tooling→product
//!   and never the reverse).
//!
//! Std-only and pure by construction: every input is a parameter, nothing here runs
//! `git`, reads the environment, or touches the filesystem. That is what makes the rung
//! order, the validator, the decoding of the raw inputs, and the build script's re-run
//! inputs unit-testable, and what lets a build script and a packaging tool share it
//! verbatim.
//!
//! Doc links here point only at items in this file: the file is compiled under three
//! different module paths, so a link to anything outside it would break in two of them.

use std::borrow::Cow;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// The base version when nothing better is known — the workspace placeholder
/// (`version = "0.0.0"` in the root `Cargo.toml`), the same fallback `cargo xtask dist`
/// passes to [`normalize_describe`].
pub const FALLBACK_BASE: &str = "0.0.0";

/// The longest identity accepted, in bytes. A Docker tag holds at most 128 characters and
/// `cargo xtask dist` tags `wyrd:<identity>-<flavor>`, so the identity leaves room for the
/// `-` and a flavor suffix.
pub const MAX_IDENTITY_LEN: usize = 100;

/// Normalize `git describe --tags --always [--dirty]` output into an artifact version.
/// Pure — unit-tested in `ci`.
///
/// * no tags yet (`git describe --always` prints a bare short sha, optionally
///   `-dirty`): `0.0.0+git.<sha>[.dirty]` — the workspace's own 0.0.0 stays the
///   base, the sha disambiguates;
/// * exactly on a tag `v0.1.0`: `0.1.0`;
/// * past a tag `v0.1.0-3-gabc12de[-dirty]`: `0.1.0+git.3.abc12de[.dirty]`.
pub fn normalize_describe(describe: &str, fallback: &str) -> String {
    let d = describe.trim();
    let (d, dirty) = match d.strip_suffix("-dirty") {
        Some(clean) => (clean, true),
        None => (d, false),
    };
    let dirty_suffix = if dirty { ".dirty" } else { "" };
    if let Some(tagged) = d.strip_prefix('v') {
        // `v0.1.0` or `v0.1.0-3-gabc12de`.
        let mut parts = tagged.rsplitn(3, '-');
        let (gsha, count) = (parts.next(), parts.next());
        if let (Some(gsha), Some(count), Some(base)) = (gsha, count, parts.next()) {
            if let Some(sha) = gsha.strip_prefix('g') {
                if count.chars().all(|c| c.is_ascii_digit()) {
                    return format!("{base}+git.{count}.{sha}{dirty_suffix}");
                }
            }
        }
        if dirty {
            return format!("{tagged}+git.dirty");
        }
        return tagged.to_string();
    }
    if d.is_empty() {
        return format!("{fallback}+git.unknown{dirty_suffix}");
    }
    // Bare short sha — no tag reachable.
    format!("{fallback}+git.{d}{dirty_suffix}")
}

/// Check that `identity` is usable everywhere it is consumed — in a log field, in a
/// tarball name, and (after `cargo xtask dist` maps `+` to `-`) as a Docker tag, whose
/// grammar is `[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}`. So: non-empty, at most
/// [`MAX_IDENTITY_LEN`] bytes, no leading `-`, `.` or `+`, and every byte in
/// `[A-Za-z0-9_.+-]`. A failing value is REFUSED, never repaired: replacing bad bytes
/// with a shared character would let two distinct inputs become one identity.
pub fn validate_identity(identity: &str) -> Result<(), String> {
    if identity.is_empty() {
        return Err("the build identity is empty".to_string());
    }
    if identity.len() > MAX_IDENTITY_LEN {
        return Err(format!(
            "the build identity is {} bytes, over the {MAX_IDENTITY_LEN}-byte cap",
            identity.len()
        ));
    }
    if identity.starts_with(['-', '.', '+']) {
        return Err(format!(
            "the build identity `{identity}` starts with `-`, `.` or `+`, which a Docker tag \
             may not"
        ));
    }
    if let Some(bad) = identity
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '+' | '-')))
    {
        return Err(format!(
            "the build identity `{}` carries `{}`, which is not legal in a Docker tag \
             (allowed: A-Z a-z 0-9 _ . + -)",
            identity.escape_debug(),
            bad.escape_debug()
        ));
    }
    Ok(())
}

/// Which rung of the resolution order produced an identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rung {
    /// Rung 1: `WYRD_VERSION` from the build environment, used verbatim — how
    /// `cargo xtask dist` hands in the version it already wrote to the tarball's `VERSION`.
    Override,
    /// Rung 2: `git describe --tags --always` of the workspace's own repository (no
    /// `--dirty`), normalized by [`normalize_describe`].
    Describe,
    /// Rung 2, failed closed: `git describe` gave no usable identity (an exotic or
    /// undecodable tag name, one that would read as a dirty-tree claim, or no answer at
    /// all) while the commit's short sha was readable, so the identity is the sha-derived
    /// form `<fallback>+git.<short sha>`.
    ShaFallback,
    /// Rung 3: nothing was known — `<fallback>+git.unknown`.
    Fallback,
}

/// The resolved build identity, the rung it came from, and any warnings the caller (the
/// build script) must surface as `cargo:warning=` lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub identity: String,
    pub rung: Rung,
    pub warnings: Vec<String>,
}

/// Read the `WYRD_VERSION` build variable as rung 1 takes it, from what the environment
/// holds (`std::env::var_os`). Pure.
///
/// Unset is `Ok(None)`. A set value is passed on as text, empty included ([`resolve`]
/// treats empty as unset). A value that is NOT UTF-8 is an `Err`, never "unset": it is an
/// explicit hand-off, and reading it as absent would silently bake a git-derived or
/// fallback identity in its place — the substitute [`resolve`] refuses for an invalid
/// UTF-8 value.
pub fn override_from_env(raw: Option<&OsStr>) -> Result<Option<&str>, String> {
    match raw {
        None => Ok(None),
        Some(raw) => raw
            .to_str()
            .map(Some)
            .ok_or_else(|| format!("WYRD_VERSION is not usable: {raw:?} is not valid UTF-8")),
    }
}

/// Decode the stdout of `git describe --tags --always` for [`resolve`]. Pure.
///
/// Git accepts any byte from 0x80 up in a tag name, so the output need not be UTF-8. An
/// undecodable output is still an answer: dropping it as absent would discard a readable
/// commit sha and fall to rung 3. It is decoded lossily instead. Each invalid sequence
/// becomes U+FFFD, which [`validate_identity`] refuses, so [`resolve`] fails closed to the
/// sha form and its warning shows what git printed. The replacement character never
/// reaches an identity, so two distinct undecodable tags cannot collapse into one.
pub fn describe_text(stdout: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(stdout)
}

/// Resolve the build identity from the three rungs, in order. Pure — the build script is
/// a thin caller that only gathers the inputs.
///
/// * `override_value` — the `WYRD_VERSION` build variable. An unset OR empty value falls
///   through to rung 2 (the Dockerfile's `ARG` convention: an unset build argument must
///   never bake an empty identity). A non-empty value is used VERBATIM — a `.dirty`
///   suffix included, because that is how a `dist` build on a dirty tree stays
///   byte-equal to its own `VERSION` — and an invalid one is an `Err`: it is an explicit
///   hand-off, and a silent substitute would break exactly the equality it exists for.
/// * `describe` — `git describe --tags --always` of the workspace's own repository, run
///   WITHOUT `--dirty`. The identity names the COMMIT the build was made from and makes
///   no claim about the working tree, so this rung never produces an identity containing
///   `dirty` (in any letter case): with no `--dirty` passed, such an identity can only
///   come from a tag whose own name carries the word (`v0.1.0-dirty`, `v0.1.0+git.dirty`,
///   `v0.1.0.dirty`), and it would read as — or even byte-equal — `dist`'s name for a
///   dirty release. It fails closed to the sha form, like any value that does not
///   validate. The build script decodes it with [`describe_text`], so a tag that is not
///   UTF-8 arrives here, and fails closed, rather than going missing.
/// * `short_sha` — `git rev-parse --short HEAD`. Used for that fail-closed form, and also
///   when `describe` gave no answer at all: a readable sha is never discarded, so rung 3
///   is reached only when the commit cannot be named.
/// * `fallback` — the base version ([`FALLBACK_BASE`] in production).
///
/// Rung 3 always carries a warning: a binary that cannot name its commit should say so
/// in the build log (an image build that was not handed `WYRD_VERSION`, for one).
pub fn resolve(
    override_value: Option<&str>,
    describe: Option<&str>,
    short_sha: Option<&str>,
    fallback: &str,
) -> Result<Resolved, String> {
    if let Some(value) = override_value.filter(|v| !v.is_empty()) {
        validate_identity(value).map_err(|e| format!("WYRD_VERSION is not usable: {e}"))?;
        return Ok(Resolved {
            identity: value.to_string(),
            rung: Rung::Override,
            warnings: Vec::new(),
        });
    }

    let mut warnings = Vec::new();
    let describe = describe.map(str::trim).filter(|d| !d.is_empty());
    match describe {
        Some(describe) => {
            let derived = normalize_describe(describe, fallback);
            let refusal = if derived.to_ascii_lowercase().contains("dirty") {
                Some(format!(
                    "`git describe` printed `{describe}`, which would make the identity \
                     `{derived}` read as a dirty-tree claim; the identity names a commit and \
                     makes no working-tree claim"
                ))
            } else {
                validate_identity(&derived)
                    .err()
                    .map(|e| format!("`git describe` printed `{describe}`: {e}"))
            };
            let Some(why) = refusal else {
                return Ok(Resolved {
                    identity: derived,
                    rung: Rung::Describe,
                    warnings,
                });
            };
            warnings.push(format!("{why}; falling back to the sha-derived identity"));
        }
        None => warnings.push(
            "neither `WYRD_VERSION` nor `git describe` of the workspace's own repository \
             is available; falling back to the sha-derived identity"
                .to_string(),
        ),
    }

    let sha_form = short_sha
        .map(str::trim)
        .filter(|s| is_short_sha(s))
        .map(|sha| format!("{fallback}+git.{sha}"))
        .filter(|id| validate_identity(id).is_ok());
    if let Some(identity) = sha_form {
        return Ok(Resolved {
            identity,
            rung: Rung::ShaFallback,
            warnings,
        });
    }
    warnings.push("no usable short sha either".to_string());

    let identity = normalize_describe("", fallback);
    warnings.push(format!(
        "the binary cannot name the commit it was built from and records `{identity}`; a \
         build whose context has no `.git/` should pass `WYRD_VERSION`"
    ));
    Ok(Resolved {
        identity,
        rung: Rung::Fallback,
        warnings,
    })
}

/// A plausible abbreviated object name: 4 to 64 lowercase hex digits.
fn is_short_sha(s: &str) -> bool {
    (4..=64).contains(&s.len()) && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The repository paths rung 2's answer depends on — what the build script hands cargo
/// as `rerun-if-changed`, given the repository's per-worktree `git_dir` (`git rev-parse
/// --git-dir`) and its `common_dir` (`--git-common-dir`; the same directory outside a
/// linked worktree). Pure. Most repositories lack some of these paths (a files-backend
/// repository has no `reftable/`, a full clone no `shallow`), and an absent one matters
/// as much as a present one: its appearance changes the answer. So the caller watches a
/// path that exists directly and one that does not for its appearance — it never drops
/// one (`crates/server/build.rs`).
///
/// `git describe --tags --always` of `HEAD` is decided by three things, and the set
/// covers each in every ref storage format git ships:
///
/// * which commit `HEAD` names — `<git_dir>/HEAD`, or, in a reftable repository (whose
///   `HEAD` file is a fixed stub), the per-worktree `<git_dir>/reftable/`;
/// * which tags exist — `<common_dir>/refs/` and `packed-refs` (the files backend) or
///   `<common_dir>/reftable/` (`git init --ref-format=reftable`);
/// * which history `describe` can walk — `<common_dir>/shallow`: deepening a shallow
///   clone can bring an existing tag into reach, and making a full clone shallow can
///   take one out of reach, without moving `HEAD` or any ref.
///
/// Deliberately NOT the index, the working tree, or `config`. The identity names a
/// commit and makes no working-tree claim, so an edit or a `git status` (which rewrites
/// the index) must not re-run the build script — that would recompile and relink this
/// crate and every integration-test binary on the ordinary edit→test loop, to refresh a
/// value that does not depend on either. None of the paths here changes on that loop.
pub fn watch_candidates(git_dir: &Path, common_dir: &Path) -> Vec<PathBuf> {
    let mut paths = vec![
        git_dir.join("HEAD"),
        git_dir.join("reftable"),
        common_dir.join("refs"),
        common_dir.join("packed-refs"),
        common_dir.join("reftable"),
        common_dir.join("shallow"),
    ];
    // Outside a linked worktree the two dirs coincide; name each path once.
    let mut seen = Vec::with_capacity(paths.len());
    paths.retain(|p| {
        if seen.contains(p) {
            false
        } else {
            seen.push(p.clone());
            true
        }
    });
    paths
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve_ok(
        override_value: Option<&str>,
        describe: Option<&str>,
        short_sha: Option<&str>,
    ) -> Resolved {
        resolve(override_value, describe, short_sha, FALLBACK_BASE).expect("resolves")
    }

    // ── rung 1: the dist hand-off ──────────────────────────────────────────────

    #[test]
    fn an_override_is_used_verbatim_including_a_dirty_suffix() {
        // A `dist` build on a dirty tree writes `….dirty` to VERSION and hands the SAME
        // string in; the binary must carry it byte for byte.
        for value in [
            "0.0.0+git.abc12de.dirty",
            "0.1.0+git.3.abc12de.dirty",
            "0.1.0+git.dirty",
            "0.1.0",
        ] {
            let r = resolve_ok(Some(value), Some("ffff000"), Some("ffff000"));
            assert_eq!(r.identity, value);
            assert_eq!(r.rung, Rung::Override);
            assert!(r.warnings.is_empty());
        }
    }

    #[test]
    fn an_unset_or_empty_override_falls_through_to_git() {
        for unset in [None, Some("")] {
            let r = resolve_ok(unset, Some("abc12de"), Some("abc12de"));
            assert_eq!(r.identity, "0.0.0+git.abc12de");
            assert_eq!(r.rung, Rung::Describe);
            assert!(r.warnings.is_empty(), "{:?}", r.warnings);
        }
    }

    #[test]
    fn an_invalid_override_is_an_error_never_a_substitute() {
        for bad in [
            "-1.0", ".1.0", "+git.abc", "1.0 beta", "1.0/x", "1.0\n", "1.0:x", "1.0é",
        ] {
            let err =
                resolve(Some(bad), Some("abc12de"), Some("abc12de"), FALLBACK_BASE).expect_err(bad);
            assert!(err.contains("WYRD_VERSION"), "{err}");
        }
        let long = "1".repeat(MAX_IDENTITY_LEN + 1);
        assert!(resolve(Some(&long), None, None, FALLBACK_BASE).is_err());
    }

    // ── rung 2: the local derivation names a commit, never a working tree ────────

    #[test]
    fn the_describe_rung_uses_the_shared_normalizer() {
        assert_eq!(
            resolve_ok(None, Some("abc12de\n"), Some("abc12de")).identity,
            "0.0.0+git.abc12de"
        );
        assert_eq!(
            resolve_ok(None, Some("v0.1.0"), Some("abc12de")).identity,
            "0.1.0"
        );
        let past = resolve_ok(None, Some("v0.1.0-3-gabc12de"), Some("abc12de"));
        assert_eq!(past.identity, "0.1.0+git.3.abc12de");
        assert_eq!(past.rung, Rung::Describe);
        assert_eq!(
            past.identity,
            normalize_describe("v0.1.0-3-gabc12de", FALLBACK_BASE)
        );
    }

    #[test]
    fn the_describe_rung_never_produces_a_dirty_identity() {
        // `--dirty` is never passed, so `dirty` can only arrive inside a tag name. Whatever
        // the tag, no `dirty` reaches the identity from rung 2: each of these fails closed
        // to the commit's sha form, with a warning.
        for describe in [
            // what `--dirty` would have appended
            "abc12de-dirty",
            "v0.1.0-dirty",
            "v0.1.0-3-gabc12de-dirty",
            // tags that carry the marker in their own name
            "v0.1.0+git.dirty",
            "v0.1.0.dirty",
            "v0.1.0.dirty-3-gabc12de",
            "vrelease-dirty-3-gabc12de",
            "vrelease-dirty-3-gabc12de-dirty",
            "v0.1.0.DIRTY",
            "v0.1.0+git.Dirty",
            "dirty",
        ] {
            let r = resolve_ok(None, Some(describe), Some("abc12de"));
            assert_eq!(
                r.identity, "0.0.0+git.abc12de",
                "`{describe}` must fail closed to the sha form"
            );
            assert_eq!(r.rung, Rung::ShaFallback, "`{describe}`");
            assert!(
                r.warnings.iter().any(|w| w.contains("dirty-tree claim")),
                "`{describe}`: {:?}",
                r.warnings
            );
        }
        // The decisive collision: `v0.1.0+git.dirty` on a CLEAN tree would otherwise be
        // byte-equal to what `dist` writes to VERSION for a DIRTY tree on `v0.1.0`.
        let dist_dirty_release = normalize_describe("v0.1.0-dirty", FALLBACK_BASE);
        assert_ne!(
            resolve_ok(None, Some("v0.1.0+git.dirty"), Some("abc12de")).identity,
            dist_dirty_release
        );
        // A clean describe still comes through untouched.
        for clean in ["abc12de", "v0.1.0", "v0.1.0-3-gabc12de"] {
            let r = resolve_ok(None, Some(clean), Some("abc12de"));
            assert_eq!(r.rung, Rung::Describe, "`{clean}`");
            assert!(!r.identity.to_ascii_lowercase().contains("dirty"));
        }
    }

    #[test]
    fn an_exotic_tag_fails_closed_to_the_sha_form_with_a_warning() {
        // `v.1` → `.1` (leading dot); `v-x` → `-x` (leading dash); `v+x` → `+x`;
        // `v1.0/rc` / `v1.0:rc` carry bytes no Docker tag may hold; an over-long tag.
        let long_tag = format!("v{}", "1".repeat(MAX_IDENTITY_LEN + 1));
        for describe in [
            "v.1", "v-x", "v+x", "v1.0/rc", "v1.0:rc", "v1.0@x", &long_tag,
        ] {
            let r = resolve_ok(None, Some(describe), Some("abc12de"));
            assert_eq!(r.identity, "0.0.0+git.abc12de", "`{describe}`");
            assert_eq!(r.rung, Rung::ShaFallback);
            assert!(
                r.warnings.iter().any(|w| w.contains("sha-derived")),
                "{:?}",
                r.warnings
            );
        }
    }

    #[test]
    fn distinct_exotic_tags_are_never_collapsed_into_a_shared_replacement() {
        // The failure v3 found: mapping bad bytes onto one character lets `1.0/rc` and
        // `1.0:rc` become the same `1.0-rc`. Fail-closed names the COMMIT instead, so the
        // two tags on two different commits stay two identities.
        let a = resolve_ok(None, Some("v1.0/rc"), Some("aaaa111"));
        let b = resolve_ok(None, Some("v1.0:rc"), Some("bbbb222"));
        assert_ne!(a.identity, b.identity);
        for r in [&a, &b] {
            assert!(!r.identity.starts_with("1.0"), "{}", r.identity);
        }
    }

    #[test]
    fn a_readable_sha_is_used_when_describe_gave_no_answer() {
        // `describe` failing while `rev-parse --short HEAD` answers must still name the
        // commit; rung 3 is for a commit that cannot be named at all.
        for describe in [None, Some(""), Some("  \n")] {
            let r = resolve_ok(None, describe, Some("abc12de"));
            assert_eq!(r.identity, "0.0.0+git.abc12de", "{describe:?}");
            assert_eq!(r.rung, Rung::ShaFallback, "{describe:?}");
            assert!(
                r.warnings.iter().any(|w| w.contains("sha-derived")),
                "{describe:?}: {:?}",
                r.warnings
            );
        }
    }

    // ── the raw inputs: decoding never turns an answer into "absent" ─────────────

    #[test]
    fn an_undecodable_tag_fails_closed_to_the_sha_form_and_never_goes_missing() {
        // Git accepts any byte from 0x80 up in a tag name. Exactly on such a tag, past
        // one, and a tag of nothing but such bytes: each names the commit by its sha.
        for stdout in [&b"v1.0\xff\n"[..], b"v1.0\xff-1-g9237986\n", b"v\xfe\xff\n"] {
            let describe = describe_text(stdout);
            let r = resolve_ok(None, Some(&describe), Some("9237986"));
            assert_eq!(r.identity, "0.0.0+git.9237986", "{stdout:?}");
            assert_eq!(r.rung, Rung::ShaFallback, "{stdout:?}");
            assert!(
                r.warnings.iter().any(|w| w.contains("sha-derived")),
                "{stdout:?}: {:?}",
                r.warnings
            );
        }
        // Two distinct undecodable tags on two commits stay two identities: the
        // replacement character never reaches an identity, so it cannot collapse them.
        let a = resolve_ok(None, Some(&describe_text(b"v1.0\xfe")), Some("aaaa111"));
        let b = resolve_ok(None, Some(&describe_text(b"v1.0\xff")), Some("bbbb222"));
        assert_ne!(a.identity, b.identity);
        // Valid UTF-8 passes through byte for byte, without a copy.
        assert!(matches!(
            describe_text(b"v0.1.0-3-gabc12de\n"),
            Cow::Borrowed("v0.1.0-3-gabc12de\n")
        ));
    }

    #[test]
    fn the_override_variable_is_passed_on_as_text_and_unset_is_none() {
        assert_eq!(override_from_env(None), Ok(None));
        // Empty is passed on; `resolve` treats it as unset (the Dockerfile `ARG` default).
        assert_eq!(override_from_env(Some(OsStr::new(""))), Ok(Some("")));
        assert_eq!(
            override_from_env(Some(OsStr::new("0.1.0+git.3.abc12de.dirty"))),
            Ok(Some("0.1.0+git.3.abc12de.dirty"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_override_that_is_not_utf8_is_an_error_never_unset() {
        use std::os::unix::ffi::OsStrExt;
        for bytes in [&b"1.2.3\xff"[..], b"\xfe\xff", b"0.1.0+git.\x80"] {
            let err = override_from_env(Some(OsStr::from_bytes(bytes)))
                .expect_err("a value that is not UTF-8 must be refused, not read as unset");
            assert!(
                err.contains("WYRD_VERSION") && err.contains("not valid UTF-8"),
                "{err}"
            );
        }
    }

    // ── rung 3 ──────────────────────────────────────────────────────────────────

    #[test]
    fn nothing_known_is_the_fallback_shape_dist_already_uses_and_is_warned() {
        let r = resolve_ok(None, None, None);
        assert_eq!(r.identity, "0.0.0+git.unknown");
        assert_eq!(r.identity, normalize_describe("", FALLBACK_BASE));
        assert_eq!(r.rung, Rung::Fallback);
        assert!(
            r.warnings.iter().any(|w| w.contains("WYRD_VERSION")),
            "rung 3 must say how to avoid it: {:?}",
            r.warnings
        );
        assert_eq!(resolve_ok(None, Some("  \n"), None).rung, Rung::Fallback);
        // An exotic tag AND no usable sha: rung 3, warned, never the bad value.
        let r = resolve_ok(None, Some("v.1"), Some("not-a-sha"));
        assert_eq!(r.identity, "0.0.0+git.unknown");
        assert_eq!(r.rung, Rung::Fallback);
        assert!(r.warnings.len() >= 3, "{:?}", r.warnings);
    }

    // ── the validator ───────────────────────────────────────────────────────────

    #[test]
    fn the_validator_accepts_every_normalizer_shape() {
        for ok in [
            "0.0.0+git.abc12de",
            "0.0.0+git.abc12de.dirty",
            "0.1.0",
            "0.1.0+git.3.abc12de",
            "0.1.0+git.dirty",
            "0.0.0+git.unknown",
            "1.0.0-rc.1_x",
        ] {
            assert_eq!(validate_identity(ok), Ok(()), "{ok}");
        }
        assert!(validate_identity(&"1".repeat(MAX_IDENTITY_LEN)).is_ok());
    }

    #[test]
    fn the_validator_refuses_what_a_docker_tag_cannot_hold() {
        for bad in [
            "", "-1", ".1", "+1", "1 2", "1/2", "1:2", "1@2", "1\t2", "1é",
        ] {
            assert!(validate_identity(bad).is_err(), "`{bad}` must be refused");
        }
        assert!(validate_identity(&"1".repeat(MAX_IDENTITY_LEN + 1)).is_err());
    }

    // ── the build script's re-run inputs ────────────────────────────────────────

    #[test]
    fn the_watch_set_covers_every_ref_format_and_the_shallow_boundary() {
        // A linked worktree: HEAD (and a reftable worktree's own stack) per worktree,
        // everything else in the common dir.
        let wt = Path::new("/repo/.git/worktrees/wt");
        let common = Path::new("/repo/.git");
        let watched = watch_candidates(wt, common);
        for expected in [
            "/repo/.git/worktrees/wt/HEAD",
            "/repo/.git/worktrees/wt/reftable",
            "/repo/.git/refs",
            "/repo/.git/packed-refs",
            "/repo/.git/reftable",
            "/repo/.git/shallow",
        ] {
            assert!(
                watched.contains(&PathBuf::from(expected)),
                "{expected} must be watched: {watched:?}"
            );
        }
        // A plain checkout: the two dirs coincide and each path is named once.
        let plain = watch_candidates(common, common);
        assert_eq!(
            plain,
            ["HEAD", "reftable", "refs", "packed-refs", "shallow"]
                .map(|p| common.join(p))
                .to_vec()
        );
    }

    #[test]
    fn the_watch_set_never_includes_the_index_the_config_or_the_work_tree() {
        // Decision 1: the identity names a commit, so nothing that changes on an edit or a
        // `git status` may re-run the build script.
        let wt = Path::new("/repo/.git/worktrees/wt");
        let common = Path::new("/repo/.git");
        for watched in watch_candidates(wt, common) {
            assert!(
                watched.starts_with(wt) || watched.starts_with(common),
                "{} is outside the repository's own metadata",
                watched.display()
            );
            let name = watched.file_name().and_then(|n| n.to_str()).unwrap_or("");
            assert!(
                !matches!(name, "index" | "config" | "logs" | "objects" | "worktrees"),
                "{} must not be watched",
                watched.display()
            );
        }
        // And never the common dir or the git dir as a whole: a directory watch is
        // recursive in cargo, so it would take in the index (a linked worktree keeps its
        // own index under its git dir).
        for dir in [wt, common] {
            assert!(!watch_candidates(wt, common).contains(&dir.to_path_buf()));
        }
        assert!(!watch_candidates(common, common).contains(&common.to_path_buf()));
    }
}
