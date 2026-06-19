//! Run the committed conformance vectors against the reference reader.
//!
//! A conforming reader MUST parse and verify every vector under `vectors/` to
//! match its `.expected.json`, and MUST reject every fragment under `invalid/`
//! (`docs/design/specs/conformance/v1.md`). This is wired into `cargo xtask ci`,
//! so a spec/implementation drift fails the build.

use std::fs;
use std::path::{Path, PathBuf};

use wyrd_chunk_format::decode;

use crate::vectors::{invalid_dir, valid_dir, ExpectedFragment};

/// Check every valid and invalid vector. Returns an error describing the first
/// failure, or the count of vectors that passed.
pub fn run() -> Result<(), String> {
    let valid = list_fragments(&valid_dir())?;
    let invalid = list_fragments(&invalid_dir())?;
    if valid.is_empty() || invalid.is_empty() {
        return Err(format!(
            "expected at least one valid and one invalid vector, found {} valid and {} invalid",
            valid.len(),
            invalid.len()
        ));
    }

    for fragment in &valid {
        check_valid(fragment)?;
    }
    for fragment in &invalid {
        check_invalid(fragment)?;
    }

    println!(
        "xtask conformance: {} valid + {} invalid vectors pass",
        valid.len(),
        invalid.len()
    );
    Ok(())
}

/// A valid fragment must decode and match its `.expected.json` exactly.
fn check_valid(fragment: &Path) -> Result<(), String> {
    let name = stem(fragment);
    let bytes = read_bytes(fragment)?;
    let decoded =
        decode(&bytes).map_err(|e| format!("valid vector `{name}` failed to decode: {e}"))?;
    let actual = ExpectedFragment::from_decoded(&decoded);

    let expected_path = fragment.with_extension("expected.json");
    let expected_text = read_text(&expected_path)?;
    let expected: ExpectedFragment = serde_json::from_str(&expected_text)
        .map_err(|e| format!("{}: invalid expected.json: {e}", expected_path.display()))?;

    if actual != expected {
        return Err(format!(
            "valid vector `{name}` decoded differently than its expected.json:\n  expected: {expected:?}\n  actual:   {actual:?}"
        ));
    }
    Ok(())
}

/// An invalid fragment must be rejected with the variant named on the first line
/// of its `.reason.txt` (`error: <Variant>`), so it cannot pass by being
/// rejected for the wrong reason.
fn check_invalid(fragment: &Path) -> Result<(), String> {
    let name = stem(fragment);
    let bytes = read_bytes(fragment)?;
    let reason_path = fragment.with_extension("reason.txt");
    let want = expected_variant(&read_text(&reason_path)?, &reason_path)?;

    match decode(&bytes) {
        Ok(_) => Err(format!(
            "invalid vector `{name}` was accepted but must be rejected ({want})"
        )),
        Err(e) if e.variant_name() == want => Ok(()),
        Err(e) => Err(format!(
            "invalid vector `{name}` rejected with `{}` but its reason.txt expects `{want}`",
            e.variant_name()
        )),
    }
}

/// Parse the `error: <Variant>` token from the first line of a reason file.
fn expected_variant(reason: &str, path: &Path) -> Result<String, String> {
    reason
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("error:"))
        .map(|v| v.trim().to_string())
        .ok_or_else(|| format!("{}: first line must be `error: <Variant>`", path.display()))
}

/// The `*.fragment` files in a directory, sorted for deterministic output.
fn list_fragments(dir: &Path) -> Result<Vec<PathBuf>, String> {
    if !dir.is_dir() {
        return Err(format!("vector directory not found: {}", dir.display()));
    }
    let mut fragments: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|ext| ext == "fragment"))
        .collect();
    fragments.sort();
    Ok(fragments)
}

fn stem(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn read_bytes(path: &Path) -> Result<Vec<u8>, String> {
    fs::read(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn read_text(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}
