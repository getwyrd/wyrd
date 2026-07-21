//! Repo-hygiene guards (#616): two invariants that review kept re-finding or
//! that would be expensive to discover late, made mechanical in the ADR-0016
//! single-source style (alongside `run_statics` and `deploy_guard`).
//!
//! **(1) No stray gitlinks.** Four PRs (#594, #595, #597, #600) accidentally
//! committed `.claude/worktrees/*` gitlink entries — mode-160000 index records
//! with no `.gitmodules` declaration — which break fresh clones and had to be
//! caught by review each time. [`scan_gitlinks`] parses NUL-delimited
//! `git ls-files -s -z` output (raw paths — the `-z` form never applies
//! `core.quotePath` C-quoting, so a non-ASCII path cannot dodge the prefix
//! check) and flags (a) any mode-160000 entry whose path is not a declared
//! submodule, and (b) ANY tracked path under `.claude/worktrees/` — agent
//! worktrees are never repository content. Declared submodule paths come from
//! `git config -z -f .gitmodules` (parsed by [`gitmodules_config_paths`]), so
//! git itself handles config quoting/escaping rather than an ad-hoc parser.
//! Both functions are pure text → the flippable test feeds synthetic listings;
//! planting a real gitlink in a fixture would itself commit the accident the
//! guard exists to prevent.
//!
//! **(2) `#![forbid(unsafe_code)]` in every crate root.** The workspace
//! convention is that every crate under `crates/` forbids unsafe code;
//! `metadata-fdb` holds the sole FFI-motivated exception at `deny` (its
//! `lib.rs` needs one audited `#[allow(unsafe_code)]` for the C bindings).
//! Nothing enforced the convention on *new* crates, and the two newest
//! (`gateway-core`, `gateway-s3`) shipped without the attribute — exactly the
//! drift this gate exists to stop. [`scan_crate_roots`] scans each crate root
//! file (`src/lib.rs` / `src/main.rs`) for the required attribute.

use std::path::Path;

/// Strip `//` line comments and (nesting-aware) `/* */` block comments, so an
/// attribute that was commented OUT — inactive to rustc — cannot satisfy the
/// unsafe-code scan by raw text match. Deliberately NOT a full lexer: a string
/// literal containing `/*` can over-strip, and an attribute spelled inside a
/// raw string would still count as present — both require writing the evasion
/// on purpose; the guard's threat model is the accidental drift (a
/// commented-out attribute that keeps its line intact), which this closes.
fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    let mut depth = 0usize;
    while let Some(c) = chars.next() {
        match (c, chars.peek(), depth) {
            ('/', Some('*'), _) => {
                chars.next();
                depth += 1;
            }
            ('*', Some('/'), 1..) => {
                chars.next();
                depth -= 1;
            }
            ('/', Some('/'), 0) => {
                for n in chars.by_ref() {
                    if n == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            (_, _, 1..) => {
                if c == '\n' {
                    out.push('\n');
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// The tracked prefix that must never appear in the index: per-task agent
/// worktrees live beside the repo (AGENTS.md §Worktree discipline) or under an
/// ignored `.claude/worktrees/`; a tracked entry there is always an accident.
pub const WORKTREES_PREFIX: &str = ".claude/worktrees/";

/// Crate roots exempt from `#![forbid(unsafe_code)]`, keyed by the exact
/// `crates/`-relative root path — NOT the crate name, so a future bin root in
/// an exempt crate does not silently inherit the weaker attribute. Each entry
/// carries the attribute the root must have instead and the audited reason.
/// Kept as data (the `STATICS_ALLOWLIST` style) so an exemption is a reviewed
/// one-line diff.
pub const UNSAFE_FORBID_ALLOWLIST: &[(&str, &str, &str)] = &[(
    "metadata-fdb/src/lib.rs",
    "#![deny(unsafe_code)]",
    "FFI: the FoundationDB C bindings need one audited #[allow(unsafe_code)]",
)];

/// Extract submodule `path` values from NUL-delimited
/// `git config -z -f .gitmodules --get-regexp <pattern>` output, whose record
/// shape is `<key>\n<value>\0`. Letting `git config` do the reading means
/// quoted/escaped config values (`path = "vendor/my dep"`) arrive here already
/// decoded to the raw path `git ls-files` reports.
pub fn gitmodules_config_paths(config_z: &str) -> Vec<String> {
    config_z
        .split('\0')
        .filter_map(|record| {
            let (key, value) = record.split_once('\n')?;
            (key.ends_with(".path") && !value.is_empty()).then(|| value.to_string())
        })
        .collect()
}

/// Scan NUL-delimited `git ls-files -s -z` output for stray gitlinks and
/// tracked agent worktrees, returning one violation string per hit (empty ⇒
/// clean). `declared` is the submodule path list from
/// [`gitmodules_config_paths`] (empty when no `.gitmodules` exists). Pure text
/// → the SAME function `cargo xtask ci` runs over the real index is driven by
/// the flippable test over planted listings.
pub fn scan_gitlinks(ls_files_z: &str, declared: &[String]) -> Vec<String> {
    let mut violations = Vec::new();
    for record in ls_files_z.split('\0') {
        // `git ls-files -s -z` record shape: `<mode> <object> <stage>\t<path>`,
        // with the path raw (never `core.quotePath`-quoted, unlike the newline
        // form — that quoting is what would let a non-ASCII path dodge the
        // prefix check below).
        let Some((meta, path)) = record.split_once('\t') else {
            continue;
        };
        let mode = meta.split_whitespace().next().unwrap_or_default();
        if path.starts_with(WORKTREES_PREFIX) {
            violations.push(format!(
                "{path}: tracked entry under {WORKTREES_PREFIX} (agent worktrees are never \
                 repository content)"
            ));
        } else if mode == "160000" && !declared.iter().any(|p| p == path) {
            violations.push(format!(
                "{path}: gitlink (mode 160000) with no .gitmodules declaration — breaks \
                 fresh clones"
            ));
        }
    }
    violations
}

/// Scan the built-artifact crate roots under `crates_dir` for
/// `#![forbid(unsafe_code)]`, honoring [`UNSAFE_FORBID_ALLOWLIST`]. Each
/// conventional rustc crate root is its own compilation unit needing its own
/// attribute; the scan covers everything that builds into or alongside the
/// shipped artifacts: `src/lib.rs`, `src/main.rs`, the auto-discovered bin
/// roots `src/bin/*.rs` / `src/bin/*/main.rs`, the build script `build.rs`,
/// `benches/*.rs`, and `examples/*.rs` / `examples/*/main.rs` (all compiled
/// by `--all-targets`). Deliberately OUT of scope, with reasons:
/// `tests/*.rs` (~100 integration-test roots — never shipped, still under the
/// workspace lint wall; a 100-file attribute sweep buys no shipped-path
/// safety) and custom `path =` target overrides (none exist in the workspace —
/// `server`'s `[[bin]]` points at the conventional `src/main.rs` — and
/// introducing one is a manifest diff a reviewer sees).
/// Returns one violation string per non-compliant root (empty ⇒ clean).
pub fn scan_crate_roots(crates_dir: &Path) -> Vec<String> {
    let mut roots = Vec::new();
    if let Ok(entries) = std::fs::read_dir(crates_dir) {
        for entry in entries.flatten() {
            let dir = entry.path();
            if !dir.join("Cargo.toml").is_file() {
                continue;
            }
            for root in ["src/lib.rs", "src/main.rs", "build.rs"] {
                let file = dir.join(root);
                if file.is_file() {
                    roots.push(file);
                }
            }
            // Auto-discovered binary roots: src/bin/<name>.rs and
            // src/bin/<name>/main.rs are each independent crate roots.
            if let Ok(bins) = std::fs::read_dir(dir.join("src/bin")) {
                for bin in bins.flatten() {
                    let path = bin.path();
                    if path.extension().is_some_and(|e| e == "rs") {
                        roots.push(path);
                    } else if path.join("main.rs").is_file() {
                        roots.push(path.join("main.rs"));
                    }
                }
            }
            // Bench and example roots build with the workspace
            // (`--all-targets`); both auto-discover the flat `<name>.rs` AND
            // the directory `<name>/main.rs` forms (verified via
            // `cargo metadata` target discovery).
            for sub in ["benches", "examples"] {
                if let Ok(found) = std::fs::read_dir(dir.join(sub)) {
                    for item in found.flatten() {
                        let path = item.path();
                        if path.extension().is_some_and(|e| e == "rs") {
                            roots.push(path);
                        } else if path.join("main.rs").is_file() {
                            roots.push(path.join("main.rs"));
                        }
                    }
                }
            }
        }
    }
    roots.sort();
    let mut violations = Vec::new();
    for file in roots {
        let Ok(content) = std::fs::read_to_string(&file) else {
            continue;
        };
        // Allowlist lookup by the exact `crates/`-relative root path (Path
        // comparison, so the match is separator-agnostic): only the recorded
        // root gets its recorded exception — sibling roots in the same crate
        // still require the full `forbid`.
        let rel = file.strip_prefix(crates_dir).unwrap_or(&file);
        let required = UNSAFE_FORBID_ALLOWLIST
            .iter()
            .find(|(root, _, _)| Path::new(root) == rel)
            .map(|(_, attr, _)| *attr)
            .unwrap_or("#![forbid(unsafe_code)]");
        // Comment-stripped match: a commented-out attribute is inactive to
        // rustc and must not satisfy the guard.
        if !strip_comments(&content)
            .lines()
            .any(|line| line.trim() == required)
        {
            violations.push(format!("{}: missing active `{required}`", file.display()));
        }
    }
    violations
}
