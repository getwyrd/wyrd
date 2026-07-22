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

use std::path::Path;

/// Normalize source for the preamble walk: strip `//` line comments and
/// (nesting-aware) `/* */` block comments — so an attribute commented OUT,
/// inactive to rustc, cannot satisfy the scan by raw text match — and BLANK
/// the contents of string literals, keeping their delimiters.
///
/// Blanking string bodies (rather than copying them through) is what makes the
/// walk correct on real preambles: `#![doc(html_root_url = "https://…")]` must
/// not have its `//` read as a comment, and `#![doc = "unmatched ["]` must not
/// have its bracket counted when [`preamble_contains`] balances an attribute.
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
/// `crates/`-relative root path — NOT the crate name, so a future bin root in
/// an exempt crate does not silently inherit the weaker attribute. Each entry
/// carries the attribute the root must have instead and the audited reason.
/// Kept as data (the `STATICS_ALLOWLIST` style) so an exemption is a reviewed
/// one-line diff.
pub const UNSAFE_FORBID_ALLOWLIST: &[(&str, &str, &str)] = &[(
    "crates/metadata-fdb/src/lib.rs",
    "#![deny(unsafe_code)]",
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

/// Does the CRATE-LEVEL attribute preamble of comment-stripped source contain
/// `required`? rustc only applies an inner attribute crate-wide when it
/// appears before the first item, so the walk accepts blank lines and other
/// inner attributes (`#![...]`, including ones spanning lines — tracked by
/// bracket balance) and stops at the first real item: an occurrence inside a
/// nested module, which scopes to that module only (and may even sit behind
/// `#[cfg(any())]`), never counts. Comparison is whitespace-insensitive so a
/// rustfmt re-wrap cannot defeat it.
fn preamble_contains(stripped: &str, required: &str) -> bool {
    let normalize = |s: &str| s.split_whitespace().collect::<String>();
    let wanted = normalize(required);
    let mut pending = String::new(); // an inner attribute still open across lines
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
            return false; // first real item — the crate-level preamble is over
        }
        if pending.matches(']').count() >= pending.matches('[').count() {
            if normalize(&pending) == wanted {
                return true;
            }
            pending.clear();
        }
    }
    false
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

    let mut missing = Vec::new();
    let Ok(entries) = std::fs::read_dir(crates_dir) else {
        // A missing crates/ dir is not this check's to report: the root-list
        // emptiness check in `scan_roots` already fails closed on it.
        return Ok(missing);
    };
    let mut dirs: Vec<_> = entries.flatten().map(|e| e.path()).collect();
    dirs.sort();
    for dir in dirs {
        let manifest = dir.join("Cargo.toml");
        if manifest.is_file() && !known.iter().any(|k| k == &manifest) {
            missing.push(format!(
                "{}: package is not a workspace member — nothing builds, tests or lints it \
                 (add it to [workspace] members in the root Cargo.toml)",
                manifest.display()
            ));
        }
    }
    Ok(missing)
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
        let required = UNSAFE_FORBID_ALLOWLIST
            .iter()
            .find(|(root, _, _)| Path::new(root) == rel)
            .map(|(_, attr, _)| *attr)
            .unwrap_or("#![forbid(unsafe_code)]");
        // Comment-stripped, preamble-scoped match: a commented-out attribute
        // is inactive to rustc, and one inside a nested module scopes to that
        // module only — neither satisfies the guard.
        if !preamble_contains(&strip_comments(&content), required) {
            violations.push(format!(
                "{}: missing active crate-level `{required}`",
                file.display()
            ));
        }
    }
    Ok(violations)
}
