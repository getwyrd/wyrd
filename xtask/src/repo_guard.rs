//! Repo-hygiene guards (#616, #775): three invariants that review kept
//! re-finding or that would be expensive to discover late, made mechanical in
//! the ADR-0016 single-source style (alongside `run_statics` and
//! `deploy_guard`). [`CI_GUARDS`] is the list `cargo xtask ci` runs them from,
//! kept here in the lib target as data so the gate's wiring is testable.
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
//! the INDEX blob `git config -z --blob :.gitmodules` (parsed by
//! [`gitmodules_config_paths`]), so git itself handles config
//! quoting/escaping rather than an ad-hoc parser, and both halves of the
//! check read the same snapshot the commit would carry.
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
//! drift this gate exists to stop. Target discovery comes from `cargo
//! metadata` ([`target_src_paths`]) so no manifest override or unconventional
//! layout can hide a root from the scan; [`scan_roots`] then checks each one.
//!
//! **(3) The blackbox validator links no Wyrd crate** (#775, proposal 0017
//! §9). `wyrd-validate` judges a Wyrd deployment from the outside; the moment
//! its binary links a `wyrd-*` crate it checks Wyrd's types against Wyrd's own
//! types and its verdict is self-referential. [`scan_blackbox_closure`] walks
//! the package's NORMAL dependency closure in the `--all-features` resolve
//! graph — transitively, optional feature-gated edges included — and,
//! separately, its declared normal dependencies. Dev-dependencies are
//! deliberately unconstrained: the proposal's §14 test fixtures are dev-only.
//! The input comes from `cargo metadata` ([`blackbox_metadata`]), the same
//! discovery route as (2).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::process::Command;

use serde_json::Value;

/// Normalize source for the preamble walk: strip `//` line comments and
/// (nesting-aware) `/* */` block comments — so an attribute commented OUT,
/// inactive to rustc, cannot satisfy the scan by raw text match — and BLANK
/// the contents of string literals, keeping their delimiters.
///
/// Blanking string bodies (rather than copying them through) is what makes the
/// walk correct on real preambles: `#![doc(html_root_url = "https://…")]` must
/// not have its `//` read as a comment, and `#![doc = "unmatched ["]` must not
/// have its bracket counted when [`preamble_unsafe`] balances an attribute.
/// It also means an attribute spelled inside a string no longer counts as
/// present. Normal strings honor `\` escapes; raw strings are matched by hash
/// count; inside a block comment, quotes are plain text.
///
/// Deliberately NOT a full lexer: char literals and exotic token sequences
/// below the preamble cannot affect the verdict, because stripping is
/// streaming and order-preserving and the walk stops at the first item.
fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    let mut depth = 0usize; // block-comment nesting
    while let Some(c) = chars.next() {
        if depth > 0 {
            // Inside a block comment: only nesting and newlines matter.
            match (c, chars.peek()) {
                ('/', Some('*')) => {
                    chars.next();
                    depth += 1;
                }
                ('*', Some('/')) => {
                    chars.next();
                    depth -= 1;
                }
                ('\n', _) => out.push('\n'),
                _ => {}
            }
            continue;
        }
        match (c, chars.peek()) {
            ('/', Some('*')) => {
                chars.next();
                depth = 1;
            }
            ('/', Some('/')) => {
                for n in chars.by_ref() {
                    if n == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            ('"', _) => {
                // Normal string literal: keep the delimiters, blank the body
                // (newlines preserved so line numbers do not shift).
                out.push(c);
                while let Some(s) = chars.next() {
                    match s {
                        '\\' => {
                            out.push(' ');
                            if let Some(e) = chars.next() {
                                out.push(if e == '\n' { '\n' } else { ' ' });
                            }
                        }
                        '"' => {
                            out.push('"');
                            break;
                        }
                        '\n' => out.push('\n'),
                        _ => out.push(' '),
                    }
                }
            }
            ('r', Some(&next)) if next == '"' || next == '#' => {
                // Possible raw string r"…" / r#"…"# — count hashes, then copy
                // verbatim until `"` followed by that many hashes.
                out.push(c);
                let mut hashes = 0usize;
                while chars.peek() == Some(&'#') {
                    out.push(chars.next().unwrap());
                    hashes += 1;
                }
                if chars.peek() == Some(&'"') {
                    out.push(chars.next().unwrap());
                    'raw: while let Some(s) = chars.next() {
                        if s == '"' {
                            out.push('"');
                            let mut seen = 0usize;
                            while seen < hashes {
                                if chars.peek() == Some(&'#') {
                                    out.push(chars.next().unwrap());
                                    seen += 1;
                                } else {
                                    continue 'raw;
                                }
                            }
                            break;
                        }
                        out.push(if s == '\n' { '\n' } else { ' ' });
                    }
                }
                // `r` not followed by a raw string (e.g. an identifier):
                // already emitted; the hash case without a quote emitted the
                // hashes too — both are plain tokens, scanning continues.
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
/// workspace-relative root path — NOT the crate name, so a future bin root in
/// an exempt crate does not silently inherit the weaker level. Each entry
/// carries the lint LEVEL that root must use instead (`deny`) and the audited
/// reason. The level is matched exactly: `forbid` would not satisfy a `deny`
/// requirement, because `forbid` cannot be overridden by the `#[allow]` the
/// exempt root needs — it would not compile.
/// Kept as data (the `STATICS_ALLOWLIST` style) so an exemption is a reviewed
/// one-line diff.
pub const UNSAFE_FORBID_ALLOWLIST: &[(&str, &str, &str)] = &[(
    "crates/metadata-fdb/src/lib.rs",
    "deny",
    "FFI: the FoundationDB C bindings need one audited #[allow(unsafe_code)]",
)];

/// Is `path` a tracked entry in NUL-delimited `git ls-files -s -z` output?
/// Used to decide whether an INDEX `.gitmodules` blob exists at all — the
/// gitlink guard reads declarations from the index (`git config --blob
/// :.gitmodules`), never the working tree, so both halves of the check see
/// the same snapshot the commit would carry (an unstaged declaration cannot
/// legitimize a staged gitlink).
pub fn index_has(ls_files_z: &str, path: &str) -> bool {
    ls_files_z
        .split('\0')
        .filter_map(|record| record.split_once('\t'))
        .any(|(_, p)| p == path)
}

/// Extract the *usable* submodule paths from NUL-delimited
/// `git config -z -f .gitmodules --get-regexp '^submodule\..*\.(path|url)$'`
/// output, whose record shape is `<key>\n<value>\0`. Letting `git config` do
/// the reading means quoted/escaped config values (`path = "vendor/my dep"`)
/// arrive here already decoded to the raw path `git ls-files` reports.
///
/// A path only counts when the SAME stanza also carries a non-empty `url`:
/// git refuses a path-only declaration at consumption time (`fatal: No url
/// found for submodule path '<p>' in .gitmodules` from `git clone
/// --recurse-submodules` / `submodule update --init` — verified live), so a
/// gitlink covered only by a url-less stanza still breaks fresh clones and
/// must stay a violation.
///
/// Duplicate stanza names follow git's EFFECTIVE-value semantics: for a
/// single-valued key git resolves the last definition (`git config --get`),
/// so when `[submodule "dep"]` appears twice mapping first `vendor/a` then
/// `vendor/b`, only `vendor/b` is a real mapping — `git submodule status`
/// fatals with `no submodule mapping found in .gitmodules for path
/// 'vendor/a'` (verified live). Keeping every raw `--get-regexp` row would
/// let the shadowed path legitimize a gitlink git itself cannot resolve.
pub fn gitmodules_config_paths(config_z: &str) -> Vec<String> {
    let mut path_of: Vec<(String, String)> = Vec::new(); // stanza name -> effective path
    let mut url_of: Vec<(String, String)> = Vec::new(); // stanza name -> effective url
    let upsert = |map: &mut Vec<(String, String)>, name: &str, value: &str| {
        // Last definition wins, matching `git config --get`.
        if let Some(slot) = map.iter_mut().find(|(n, _)| n == name) {
            slot.1 = value.to_string();
        } else {
            map.push((name.to_string(), value.to_string()));
        }
    };
    for record in config_z.split('\0') {
        let Some((key, value)) = record.split_once('\n') else {
            continue;
        };
        // Key shape: `submodule.<name>.path` / `submodule.<name>.url`; the
        // name may itself contain dots, so strip the known prefix/suffix.
        let Some(rest) = key.strip_prefix("submodule.") else {
            continue;
        };
        if let Some(name) = rest.strip_suffix(".path") {
            upsert(&mut path_of, name, value);
        } else if let Some(name) = rest.strip_suffix(".url") {
            upsert(&mut url_of, name, value);
        }
    }
    path_of
        .into_iter()
        .filter_map(|(name, path)| {
            let usable =
                !path.is_empty() && url_of.iter().any(|(n, url)| *n == name && !url.is_empty());
            usable.then_some(path)
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

/// The lint level (`forbid` / `deny` / `warn` / `allow`) an inner attribute
/// applies to `unsafe_code`, or `None` if it does not mention it.
///
/// A list is parsed, not a fixed spelling: `#![forbid(unsafe_code,
/// unused_imports)]` and `#![forbid(unsafe_code,)]` set the level on
/// `unsafe_code` exactly as the bare form does, while `#![forbid(dead_code)]`
/// does not mention it. Whitespace-insensitive, so a rustfmt re-wrap cannot
/// defeat it.
fn attribute_level_for_unsafe(attr: &str) -> Option<&'static str> {
    let compact: String = attr.split_whitespace().collect();
    let inner = compact
        .strip_prefix("#![")
        .and_then(|s| s.strip_suffix("]"))?;
    for level in ["forbid", "deny", "warn", "allow"] {
        if let Some(list) = inner
            .strip_prefix(level)
            .and_then(|s| s.strip_prefix('('))
            .and_then(|s| s.strip_suffix(')'))
        {
            if list.split(',').any(|lint| lint == "unsafe_code") {
                return Some(level);
            }
        }
    }
    None
}

/// Does the inner attribute conditionally set a lint level on `unsafe_code`
/// via `cfg_attr`? `#![cfg_attr(feature = "x", allow(unsafe_code))]` lowers
/// the level to `allow` whenever the predicate holds — so the crate's unsafe
/// policy varies by configuration, which the guard cannot accept: it must be
/// forbidden/denied in EVERY configuration. Detected by shape (a `cfg_attr`
/// attribute mentioning `unsafe_code`), because the guard cannot evaluate the
/// predicate.
fn attribute_conditionally_touches_unsafe(attr: &str) -> bool {
    let compact: String = attr.split_whitespace().collect();
    compact
        .strip_prefix("#![")
        .and_then(|s| s.strip_suffix("]"))
        .is_some_and(|inner| inner.starts_with("cfg_attr(") && inner.contains("unsafe_code"))
}

/// The unsafe-code verdict a crate-level preamble produces.
struct PreambleUnsafe {
    /// The last UNCONDITIONAL crate-level level applied to `unsafe_code`, or
    /// `None` if the preamble sets none.
    level: Option<&'static str>,
    /// A `cfg_attr` in the preamble sets a level on `unsafe_code` in some
    /// configuration — the policy is not uniform, so no fixed level can be
    /// certified.
    conditional: bool,
}

/// The EFFECTIVE crate-level unsafe-code verdict for a comment-stripped
/// preamble.
///
/// rustc applies the LAST level set before the first item, so the walk does
/// not stop at the first match: `#![deny(unsafe_code)]` followed by
/// `#![allow(unsafe_code)]` resolves to `allow`, and a guard that returned on
/// the deny would miss the downgrade. (Only reachable for the `deny`
/// exemption — a later override of a `forbid` does not compile.) A
/// `cfg_attr` touching `unsafe_code` makes the policy configuration-dependent
/// (`conditional`). The walk accepts a leading UTF-8 BOM (rustc does), blank
/// lines, a shebang, and inner attributes spanning lines (tracked by bracket
/// balance), and stops at the first real item: an occurrence inside a nested
/// module — even behind `#[cfg(any())]` — scopes to that module only and
/// never reaches this walk.
fn preamble_unsafe(stripped: &str) -> PreambleUnsafe {
    // rustc accepts a leading BOM; `str::trim()` does not remove U+FEFF, so an
    // un-stripped BOM would make the first line not start with `#![` and end
    // the walk before the attribute — a false red on a compliant file.
    let stripped = stripped.strip_prefix('\u{feff}').unwrap_or(stripped);
    let mut pending = String::new(); // an inner attribute still open across lines
    let mut effective = None;
    let mut conditional = false;
    for (idx, raw) in stripped.lines().enumerate() {
        let t = raw.trim();
        // A first-line `#!` that is not `#![` is a SHEBANG, which rustc skips
        // (`#!/usr/bin/env rust-script`); treating it as the first item would
        // reject a compliant crate whose attribute follows it.
        if idx == 0 && t.starts_with("#!") && !t.starts_with("#![") {
            continue;
        }
        if !pending.is_empty() {
            pending.push(' ');
            pending.push_str(t);
        } else if t.is_empty() {
            continue;
        } else if t.starts_with("#![") {
            pending = t.to_string();
        } else {
            break; // first real item — the crate-level preamble is over
        }
        if pending.matches(']').count() >= pending.matches('[').count() {
            if let Some(level) = attribute_level_for_unsafe(&pending) {
                effective = Some(level);
            } else if attribute_conditionally_touches_unsafe(&pending) {
                conditional = true;
            }
            pending.clear();
        }
    }
    PreambleUnsafe {
        level: effective,
        conditional,
    }
}

/// Extract every non-test Cargo target's `src_path` from `cargo metadata
/// --no-deps --format-version 1` output. This is the AUTHORITATIVE target
/// list: it already accounts for `[lib] path`, `package.build`, custom
/// `[[bin]]`/`[[bench]]`/`[[example]]` paths, packages nested under an
/// intermediate directory, and every conventional auto-discovered layout — so
/// the guard cannot fall behind a manifest that moves a target somewhere the
/// scan never thought to look.
///
/// EVERY target kind is in scope — `lib`, `bin`, `bench`, `example`,
/// `custom-build` and `test` alike. Integration-test roots were briefly
/// excluded on the theory that they never ship and stay covered by the
/// workspace lint wall; the second half of that is false, because
/// `warnings = "deny"` and `clippy.all = "deny"` do not forbid unsafe code.
/// Nothing else would have caught unsafe in a test, so the invariant now
/// holds uniformly: one rule, every crate root, no asterisk.
pub fn target_src_paths(metadata_json: &str) -> Result<Vec<std::path::PathBuf>, String> {
    let meta: serde_json::Value = serde_json::from_str(metadata_json)
        .map_err(|e| format!("unsafe-guard: cannot parse cargo metadata: {e}"))?;
    let packages = meta
        .get("packages")
        .and_then(|p| p.as_array())
        .ok_or_else(|| "unsafe-guard: cargo metadata has no `packages` array".to_string())?;
    let mut roots = Vec::new();
    for package in packages {
        let Some(targets) = package.get("targets").and_then(|t| t.as_array()) else {
            continue;
        };
        for target in targets {
            if let Some(path) = target.get("src_path").and_then(|s| s.as_str()) {
                roots.push(std::path::PathBuf::from(path));
            }
        }
    }
    roots.sort();
    roots.dedup();
    Ok(roots)
}

/// Package directories under `crates_dir` that `cargo metadata` did not report.
///
/// The workspace lists `[workspace] members` EXPLICITLY, and a path dependency
/// only becomes an implicit member when some member depends on it. So a new
/// `crates/foo` that nobody has wired up yet is absent from metadata — and a
/// guard driven by metadata alone would pass without ever seeing it, exactly
/// as it would pass over a crate that does not exist.
///
/// Reported as violations in their own right, because "under `crates/` but not
/// a workspace member" is a defect regardless of this guard: nothing compiles,
/// tests, or lints that directory, so it looks live while being dead.
pub fn unregistered_manifests(
    metadata_json: &str,
    crates_dir: &Path,
) -> Result<Vec<String>, String> {
    let meta: serde_json::Value = serde_json::from_str(metadata_json)
        .map_err(|e| format!("unsafe-guard: cannot parse cargo metadata: {e}"))?;
    let known: Vec<std::path::PathBuf> = meta
        .get("packages")
        .and_then(|p| p.as_array())
        .ok_or_else(|| "unsafe-guard: cargo metadata has no `packages` array".to_string())?
        .iter()
        .filter_map(|p| p.get("manifest_path").and_then(|m| m.as_str()))
        .map(std::path::PathBuf::from)
        .collect();

    // Walk the WHOLE tree, not just its immediate children: a package may sit
    // under an intermediate grouping directory (`crates/storage/foo/`), and a
    // single-level listing would miss exactly the unregistered package this
    // check exists to find.
    let mut manifests = Vec::new();
    collect_manifests(crates_dir, &mut manifests);
    manifests.sort();

    let mut missing = Vec::new();
    for manifest in manifests {
        if !known.iter().any(|k| k == &manifest) {
            missing.push(format!(
                "{}: package is not a workspace member — nothing builds, tests or lints it \
                 (add it to [workspace] members in the root Cargo.toml)",
                manifest.display()
            ));
        }
    }
    Ok(missing)
}

/// Every `Cargo.toml` at or below `dir`, at any depth. `target/` and
/// dot-directories are skipped (a build cache holds vendored manifests that
/// are not this workspace's packages). A package directory IS descended into,
/// because cargo permits a package nested inside another package's directory.
/// A missing/unreadable `dir` yields nothing — `scan_roots`' empty-root-list
/// check is what fails closed on an unseeable tree.
fn collect_manifests(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        // `file_type()` does NOT follow symlinks, unlike `Path::is_dir()`. A
        // followed directory link would collect the same manifest under a
        // second, symlink-expanded path — which can never equal cargo's
        // canonical `manifest_path`, producing a false "not a workspace
        // member" — and a link to an ancestor would recurse until the path
        // length gave out.
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if name == "target" || name.starts_with('.') {
                continue;
            }
            collect_manifests(&path, out);
        } else if path.file_name().is_some_and(|n| n == "Cargo.toml") {
            out.push(path);
        }
    }
}

/// Scan the given crate roots for `#![forbid(unsafe_code)]`, honoring
/// [`UNSAFE_FORBID_ALLOWLIST`] (keyed by workspace-root-relative path).
///
/// Fails CLOSED: an empty root list is `Err`, never a vacuously clean pass —
/// a guard that was handed nothing to check must say so. An unreadable root
/// file is reported as a violation for the same reason.
/// `Ok` carries one violation string per non-compliant root (empty ⇒ clean).
pub fn scan_roots(
    roots: &[std::path::PathBuf],
    workspace_root: &Path,
) -> Result<Vec<String>, String> {
    if roots.is_empty() {
        return Err(
            "unsafe-guard: cargo metadata yielded no crate roots — refusing to pass \
             a workspace it cannot see"
                .to_string(),
        );
    }
    let mut violations = Vec::new();
    for file in roots {
        let content = match std::fs::read_to_string(file) {
            Ok(content) => content,
            Err(e) => {
                violations.push(format!("{}: unreadable crate root: {e}", file.display()));
                continue;
            }
        };
        // Allowlist lookup by the exact workspace-relative root path (Path
        // comparison, so the match is separator-agnostic): only the recorded
        // root gets its recorded exception — sibling roots in the same crate
        // still require the full `forbid`.
        let rel = file.strip_prefix(workspace_root).unwrap_or(file);
        let level = UNSAFE_FORBID_ALLOWLIST
            .iter()
            .find(|(root, _, _)| Path::new(root) == rel)
            .map(|(_, level, _)| *level)
            .unwrap_or("forbid");
        // The EFFECTIVE preamble level must equal what the root requires, in
        // EVERY configuration. A commented-out attribute is inactive, one
        // inside a nested module scopes there only, a later override wins, and
        // a `cfg_attr` makes the level configuration-dependent — none
        // satisfies the guard. `deny` and `forbid` are distinct requirements:
        // the FFI root needs `deny` (a later item-level `#[allow]` must be
        // able to override it), every other root needs `forbid`.
        let verdict = preamble_unsafe(&strip_comments(&content));
        if verdict.conditional {
            violations.push(format!(
                "{}: crate-level unsafe-code policy is conditional — a `cfg_attr` sets a \
                 level on `unsafe_code`, so some configuration may permit it. It must be \
                 `#![{level}(unsafe_code)]` unconditionally.",
                file.display()
            ));
        } else if verdict.level != Some(level) {
            violations.push(format!(
                "{}: missing active crate-level `#![{level}(unsafe_code)]` \
                 (a lint list containing `unsafe_code` satisfies it; a later \
                 crate-level override does not)",
                file.display()
            ));
        }
    }
    Ok(violations)
}

/// One repo-hygiene guard `cargo xtask ci` runs before its cargo steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CiGuard {
    /// (1) No stray gitlinks or tracked agent worktrees (#616).
    Gitlink,
    /// (2) `#![forbid(unsafe_code)]` in every crate root (#616).
    UnsafeForbid,
    /// (3) The blackbox validator's normal dependency closure holds no Wyrd
    /// crate (#775).
    BlackboxClosure,
}

/// The guards `cargo xtask ci` runs, in order, before any cargo step.
///
/// Data in the lib target rather than a sequence of calls in `main.rs`'s
/// `run_ci`, for the same reason [`crate::feature_gated_checks`] lives here: a
/// guard that is defined and tested but never called would pass every test of
/// the guard itself. `main.rs`'s `run_ci_steps` iterates this list, so its
/// recording-executor unit tests and `cargo xtask ci-dry-run` (which
/// `xtask/tests/blackbox_dependency_guard.rs` drives over planted workspaces)
/// see exactly the guards the gate runs.
pub const CI_GUARDS: &[CiGuard] = &[
    CiGuard::Gitlink,
    CiGuard::UnsafeForbid,
    CiGuard::BlackboxClosure,
];

/// The package whose normal dependency closure must hold no Wyrd crate
/// (proposal 0017 §9).
pub const BLACKBOX_PACKAGE: &str = "wyrd-validate";

/// The package-name prefix every Wyrd workspace crate carries.
pub const WYRD_CRATE_PREFIX: &str = "wyrd-";

/// The `cargo` arguments [`blackbox_metadata`] runs.
///
/// NOT `--no-deps` (the unsafe guard's form): that omits the `resolve` graph
/// this guard walks. `--locked`, so the audit can never rewrite the
/// `Cargo.lock` it audits — a stale or missing lock file is an error instead.
/// `--all-features`, because the resolve graph depends on the feature
/// selection: an optional dependency behind an off-by-default feature (the
/// `crates/metadata-tikv/Cargo.toml` pattern) is absent from a default-feature
/// graph. Resolving is all it does — nothing is compiled, so it needs neither
/// the TiKV nor the FoundationDB toolchain.
pub const BLACKBOX_METADATA_ARGS: &[&str] = &[
    "metadata",
    "--format-version",
    "1",
    "--locked",
    "--all-features",
];

/// Run `cargo` with [`BLACKBOX_METADATA_ARGS`] in the workspace at `dir` and
/// return the JSON document it prints — the input [`scan_blackbox_closure`]
/// reads.
pub fn blackbox_metadata(dir: &Path) -> Result<String, String> {
    let meta = Command::new("cargo")
        .args(BLACKBOX_METADATA_ARGS)
        .current_dir(dir)
        .output()
        .map_err(|e| format!("blackbox-guard: failed to spawn cargo metadata: {e}"))?;
    if !meta.status.success() {
        return Err(format!(
            "blackbox-guard: `cargo {}` failed with {}:\n{}",
            BLACKBOX_METADATA_ARGS.join(" "),
            meta.status,
            String::from_utf8_lossy(&meta.stderr)
        ));
    }
    String::from_utf8(meta.stdout)
        .map_err(|e| format!("blackbox-guard: cargo metadata output is not UTF-8: {e}"))
}

/// Is a dependency `kind` from `cargo metadata` a NORMAL one? `null` is
/// normal; `"dev"` and `"build"` are not (neither links into the shipped
/// binary). A missing field or any other value is an error: the guard does
/// not guess whether a dependency it cannot classify links into the binary.
fn is_normal_kind(kind: Option<&Value>, what: &str) -> Result<bool, String> {
    match kind {
        Some(Value::Null) => Ok(true),
        Some(Value::String(k)) if k == "dev" || k == "build" => Ok(false),
        Some(other) => Err(format!(
            "blackbox-guard: {what} has unknown dependency kind {other} — refusing to guess \
             whether it links into the binary"
        )),
        None => Err(format!(
            "blackbox-guard: {what} has no `kind` field — refusing to guess whether it links \
             into the binary"
        )),
    }
}

/// Is a resolve-graph edge (`resolve.nodes[].deps[]`) a NORMAL one? An edge
/// carries one `dep_kinds` entry per way the dependency is declared (normal,
/// dev, build — each per target), so it is normal when ANY entry is.
///
/// Fails closed: a missing or EMPTY `dep_kinds` list says nothing about how
/// the dependency links, and skipping the edge would hide everything below
/// it, so both are `Err`. Every entry is classified — not just up to the
/// first normal one — so an unknown kind anywhere in the list is `Err` too.
fn edge_is_normal(edge: &Value, what: &str) -> Result<bool, String> {
    let kinds = edge
        .get("dep_kinds")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!(
                "blackbox-guard: {what} has no `dep_kinds` list (cargo too old?) — cannot \
                 tell normal from dev edges"
            )
        })?;
    if kinds.is_empty() {
        return Err(format!(
            "blackbox-guard: {what} has an empty `dep_kinds` list — refusing to guess whether \
             it links into the binary"
        ));
    }
    let mut normal = false;
    for kind in kinds {
        if is_normal_kind(kind.get("kind"), what)? {
            normal = true;
        }
    }
    Ok(normal)
}

/// What the blackbox scan found about one offending crate. Both checks can
/// fire for the same crate (a direct normal dependency is both declared and
/// in the graph); they are merged so each crate is reported once, saying
/// which checks fired.
#[derive(Default)]
struct BlackboxFinding<'a> {
    /// Declared as a normal dependency in the manifest; `true` when optional.
    declared_optional: Option<bool>,
    /// A shortest normal-edge path from [`BLACKBOX_PACKAGE`] to the crate.
    path: Option<Vec<&'a str>>,
}

/// Scan a `cargo metadata` document (produced with [`BLACKBOX_METADATA_ARGS`])
/// for Wyrd crates that [`BLACKBOX_PACKAGE`] could link, returning one
/// violation per offending crate (empty ⇒ clean). Pure text in, violations
/// out: the SAME function `cargo xtask ci` runs over the real workspace is the
/// one `cargo xtask blackbox-guard --metadata <file>` runs over a planted
/// document.
///
/// Two checks, reported as one invariant:
///
/// * **the graph** — follow only normal edges of `resolve.nodes`, transitively
///   from the blackbox package, and report every reached package whose name
///   starts with [`WYRD_CRATE_PREFIX`], with the path that reaches it;
/// * **the manifest** — the package's declared `dependencies`, which do not
///   depend on the feature selection: any normal `wyrd-*` entry, optional or
///   not. This still catches a declaration if some feature-resolution
///   subtlety keeps it out of the graph.
///
/// Fails CLOSED, like [`scan_roots`], refusing to pass a closure it cannot
/// see: a document it cannot parse, no `resolve` graph, no blackbox package
/// (or more than one), a package that is in no resolve node, a package record
/// with no decodable `id` or `name`, and an edge or declaration whose kind it
/// cannot classify are all `Err`, never a clean pass. Package names come from
/// the package records, never from parsing an id or an edge's extern name:
/// an id is opaque and an edge name follows a rename, so reading the name
/// from either could let the prefix check miss.
pub fn scan_blackbox_closure(metadata_json: &str) -> Result<Vec<String>, String> {
    let meta: Value = serde_json::from_str(metadata_json)
        .map_err(|e| format!("blackbox-guard: cannot parse cargo metadata: {e}"))?;
    let packages = meta
        .get("packages")
        .and_then(Value::as_array)
        .ok_or_else(|| "blackbox-guard: cargo metadata has no `packages` array".to_string())?;

    // Every package record's identity, decoded strictly up front.
    let mut name_of: BTreeMap<&str, &str> = BTreeMap::new();
    let mut subject = None;
    for package in packages {
        let id = package
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| "blackbox-guard: a package record has no decodable `id`".to_string())?;
        let name = package.get("name").and_then(Value::as_str).ok_or_else(|| {
            format!("blackbox-guard: package record `{id}` has no decodable `name`")
        })?;
        name_of.insert(id, name);
        if name == BLACKBOX_PACKAGE {
            if subject.is_some() {
                return Err(format!(
                    "blackbox-guard: cargo metadata has more than one `{BLACKBOX_PACKAGE}` \
                     package — cannot tell which one ships"
                ));
            }
            subject = Some((id, package));
        }
    }
    let Some((subject_id, subject)) = subject else {
        return Err(format!(
            "blackbox-guard: cargo metadata has no `{BLACKBOX_PACKAGE}` package — refusing to \
             pass a guard whose subject it cannot see (is `crates/validate` still a \
             [workspace] member?)"
        ));
    };

    let mut findings: BTreeMap<&str, BlackboxFinding> = BTreeMap::new();

    // (a) The manifest: declared normal dependencies, optional ones included.
    let declared = subject
        .get("dependencies")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!("blackbox-guard: `{BLACKBOX_PACKAGE}` has no `dependencies` list")
        })?;
    for dep in declared {
        // `name` is the PACKAGE name; a rename lives in its own `rename` field.
        let name = dep.get("name").and_then(Value::as_str).ok_or_else(|| {
            format!("blackbox-guard: a declared dependency of `{BLACKBOX_PACKAGE}` has no `name`")
        })?;
        let what = format!("declared dependency `{name}` of `{BLACKBOX_PACKAGE}`");
        if is_normal_kind(dep.get("kind"), &what)? && name.starts_with(WYRD_CRATE_PREFIX) {
            // Wording only: an optional declaration is a violation all the same.
            let optional = dep.get("optional") == Some(&Value::Bool(true));
            findings.entry(name).or_default().declared_optional = Some(optional);
        }
    }

    // (b) The graph: normal edges only, transitively. Breadth-first, so the
    // reported path is a shortest one.
    let nodes = meta
        .get("resolve")
        .and_then(|r| r.get("nodes"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            "blackbox-guard: cargo metadata has no `resolve.nodes` graph (was it run with \
             --no-deps?)"
                .to_string()
        })?;
    let mut node_of: BTreeMap<&str, &Value> = BTreeMap::new();
    for node in nodes {
        let id = node
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| "blackbox-guard: a resolve node has no decodable `id`".to_string())?;
        node_of.insert(id, node);
    }
    if !node_of.contains_key(subject_id) {
        return Err(format!(
            "blackbox-guard: `{BLACKBOX_PACKAGE}` appears in no resolve node — refusing to \
             pass a closure it cannot see"
        ));
    }
    let mut seen: BTreeSet<&str> = BTreeSet::from([subject_id]);
    let mut queue: VecDeque<(&str, Vec<&str>)> =
        VecDeque::from([(subject_id, vec![BLACKBOX_PACKAGE])]);
    while let Some((id, path)) = queue.pop_front() {
        let node = node_of.get(id).ok_or_else(|| {
            format!("blackbox-guard: package `{id}` is reached but appears in no resolve node")
        })?;
        let deps = node
            .get("deps")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("blackbox-guard: resolve node `{id}` has no `deps` list"))?;
        for dep in deps {
            let pkg = dep.get("pkg").and_then(Value::as_str).ok_or_else(|| {
                format!("blackbox-guard: an edge of resolve node `{id}` has no `pkg`")
            })?;
            // Classify before the `seen` check, so every edge of every reached
            // node is classified; and only a NORMAL edge marks its target seen,
            // so a crate first met through a dev edge is still walked when a
            // normal path reaches it.
            if !edge_is_normal(dep, &format!("edge `{id}` -> `{pkg}`"))? || !seen.insert(pkg) {
                continue;
            }
            let name = *name_of.get(pkg).ok_or_else(|| {
                format!("blackbox-guard: edge `{id}` -> `{pkg}` reaches a package with no record")
            })?;
            let mut next = path.clone();
            next.push(name);
            if name.starts_with(WYRD_CRATE_PREFIX) {
                findings.entry(name).or_default().path = Some(next.clone());
            }
            queue.push_back((pkg, next));
        }
    }

    Ok(findings
        .into_iter()
        .map(|(name, finding)| {
            let mut how = Vec::new();
            if let Some(optional) = finding.declared_optional {
                how.push(format!(
                    "declared as a normal{} dependency in its manifest",
                    if optional { " optional" } else { "" }
                ));
            }
            if let Some(path) = finding.path {
                how.push(format!(
                    "reached through the normal dependency graph (all features): {}",
                    path.join(" -> ")
                ));
            }
            format!(
                "{BLACKBOX_PACKAGE} must not link Wyrd crate `{name}`: {}",
                how.join("; ")
            )
        })
        .collect())
}
