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
//! drift this gate exists to stop. [`scan_crate_roots`] scans each crate root
//! file (`src/lib.rs` / `src/main.rs`) for the required attribute.

use std::path::Path;

/// Strip `//` line comments and (nesting-aware) `/* */` block comments, so an
/// attribute that was commented OUT — inactive to rustc — cannot satisfy the
/// unsafe-code scan by raw text match. String-aware where it matters: comment
/// markers inside string literals — `#![doc(html_root_url = "https://…")]` is
/// the canonical preamble case — are content, not comments (normal strings
/// with `\` escapes, raw strings by hash count); inside a block comment,
/// quotes are plain text. Deliberately NOT a full lexer: char literals and
/// exotic token sequences below the preamble cannot affect the preamble walk
/// (stripping is streaming and order-preserving), and an attribute spelled
/// inside a raw string still counts as present — that requires writing the
/// evasion on purpose; the threat model is accidental drift.
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
                // Normal string literal: copy verbatim through the closing
                // quote, honoring `\` escapes.
                out.push(c);
                while let Some(s) = chars.next() {
                    out.push(s);
                    match s {
                        '\\' => {
                            if let Some(e) = chars.next() {
                                out.push(e);
                            }
                        }
                        '"' => break,
                        _ => {}
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
                        out.push(s);
                        if s == '"' {
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
    "metadata-fdb/src/lib.rs",
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
    for raw in stripped.lines() {
        let t = raw.trim();
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
///
/// Fails CLOSED: an unscannable `crates_dir` or the discovery of zero crate
/// roots is `Err`, never an empty (vacuously clean) violation list — a guard
/// that cannot see the tree must say so, not pass it. An unreadable
/// individual root file is reported as a violation for the same reason.
/// `Ok` carries one violation string per non-compliant root (empty ⇒ clean).
pub fn scan_crate_roots(crates_dir: &Path) -> Result<Vec<String>, String> {
    let mut roots = Vec::new();
    {
        let entries = std::fs::read_dir(crates_dir).map_err(|e| {
            format!(
                "unsafe-guard: cannot scan {}: {e} — refusing to pass a tree it cannot see",
                crates_dir.display()
            )
        })?;
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
    if roots.is_empty() {
        return Err(format!(
            "unsafe-guard: found no crate roots under {} — a moved or empty crates tree \
             must fail the gate, not pass it vacuously",
            crates_dir.display()
        ));
    }
    roots.sort();
    let mut violations = Vec::new();
    for file in roots {
        let content = match std::fs::read_to_string(&file) {
            Ok(content) => content,
            Err(e) => {
                violations.push(format!("{}: unreadable crate root: {e}", file.display()));
                continue;
            }
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
