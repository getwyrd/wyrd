//! Regression guard for the first-class FoundationDB `wyrd` OCI image (issue #470).
//!
//! ADR-0042 chose FoundationDB as the production metadata backend, but until this
//! bundle the only Dockerfile that built `wyrd` was a TEST fixture
//! (`crates/chunkstore-grpc/tests/dserver/Dockerfile`) that could not build the `fdb`
//! feature at all. This test pins the new production image
//! (`deploy/docker/wyrd/Dockerfile`) to the shape an operator needs and — the
//! load-bearing part — mechanically couples the FoundationDB client version baked into
//! the image to BOTH the cluster the repo deploys and the crate the binary links, so a
//! silent client/cluster drift (the failure mode #441's version-skew guard diagnoses)
//! fails the gate instead of production.
//!
//! Container-free by design (ADR-0016: `cargo xtask ci` builds no image, needs no
//! network). Every assertion is a file read + substring/parse; the parsing helpers live
//! IN THIS FILE (nothing imported from `xtask`'s lib), following the
//! `xtask/tests/readme_dev_section.rs` precedent — these are file compares, not shipped
//! logic. The actual `docker build` is the deferred half, exercised by
//! `.github/workflows/fdb-image.yml`.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

const DOCKERFILE: &str = "deploy/docker/wyrd/Dockerfile";
const FDB_COMPOSE: &str = "deploy/fdb-single-node/docker-compose.yml";
const WORKFLOW: &str = ".github/workflows/fdb-image.yml";

// ─── pure parsing helpers (local to this test) ────────────────────────────────

/// The default value of `ARG FDB_VERSION=<v>` in a Dockerfile, if declared.
fn dockerfile_fdb_version(dockerfile: &str) -> Option<String> {
    for line in dockerfile.lines() {
        if let Some(rest) = line.trim().strip_prefix("ARG FDB_VERSION=") {
            let v = rest.trim().trim_matches('"');
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// The `foundationdb/foundationdb:<tag>` image tag in a compose file, if present.
fn compose_fdb_tag(compose: &str) -> Option<String> {
    const NEEDLE: &str = "foundationdb/foundationdb:";
    for line in compose.lines() {
        if let Some(idx) = line.find(NEEDLE) {
            let tag: String = line[idx + NEEDLE.len()..]
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            if !tag.is_empty() {
                return Some(tag);
            }
        }
    }
    None
}

/// The `fdb-M_N` cargo feature (`Cargo.toml:108`) rendered as a `M.N` version line.
fn cargo_fdb_major_minor(cargo_toml: &str) -> Option<String> {
    const NEEDLE: &str = "fdb-";
    let idx = cargo_toml.find(NEEDLE)?;
    let ver: String = cargo_toml[idx + NEEDLE.len()..]
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '_')
        .collect();
    if ver.is_empty() {
        return None;
    }
    Some(ver.replace('_', "."))
}

/// The `major.minor` line of a `major.minor.patch` version string.
fn major_minor(v: &str) -> String {
    v.split('.').take(2).collect::<Vec<_>>().join(".")
}

/// The load-bearing consistency check, factored so both the real-tree assertion and
/// the planted-red fixture drive the SAME code. `Ok(version)` on agreement, `Err(why)`
/// on any drift.
fn check_fdb_version_consistency(
    dockerfile: &str,
    compose: &str,
    cargo_toml: &str,
) -> Result<String, String> {
    let df = dockerfile_fdb_version(dockerfile)
        .ok_or_else(|| "Dockerfile declares no `ARG FDB_VERSION=<v>`".to_string())?;
    let tag = compose_fdb_tag(compose)
        .ok_or_else(|| "compose file has no `foundationdb/foundationdb:<tag>`".to_string())?;
    let crate_mm = cargo_fdb_major_minor(cargo_toml)
        .ok_or_else(|| "Cargo.toml has no `fdb-M_N` feature".to_string())?;

    // Exact patch must agree between the baked client and the deployed cluster.
    if df != tag {
        return Err(format!(
            "FDB_VERSION `{df}` (Dockerfile) != cluster tag `{tag}` (compose): a baked \
             client that disagrees with the deployed cluster is the #441 skew"
        ));
    }
    // major.minor must agree across all three, including the crate feature line.
    let df_mm = major_minor(&df);
    let tag_mm = major_minor(&tag);
    if df_mm != crate_mm || tag_mm != crate_mm {
        return Err(format!(
            "FDB major.minor mismatch: Dockerfile `{df_mm}`, compose `{tag_mm}`, crate \
             feature `{crate_mm}`"
        ));
    }
    Ok(df)
}

/// The `docker build … -f <path> … -t <tag>` invocations a workflow names, as
/// `(dockerfile, tag)` pairs. The workflow's `run:` blocks split a single `docker build`
/// across continuation lines, so scan the whole flattened token stream rather than
/// line-by-line.
fn docker_builds(workflow: &str) -> Vec<(Option<String>, Option<String>)> {
    let mut builds = Vec::new();
    // A `docker build` invocation starts at the `docker build` token pair and runs to
    // the end of the flattened token run; the workflow has one such invocation, but the
    // scan generalises to more.
    let toks: Vec<&str> = workflow.split_whitespace().collect();
    let mut i = 0;
    while i + 1 < toks.len() {
        if toks[i] == "docker" && toks[i + 1] == "build" {
            let mut dockerfile = None;
            let mut tag = None;
            let mut j = i + 2;
            while j < toks.len() {
                if toks[j] == "docker" && j + 1 < toks.len() && toks[j + 1] == "build" {
                    break; // next invocation
                }
                match toks[j] {
                    "-f" if j + 1 < toks.len() => dockerfile = Some(toks[j + 1].to_string()),
                    "-t" if j + 1 < toks.len() => tag = Some(toks[j + 1].to_string()),
                    _ => {}
                }
                j += 1;
            }
            builds.push((dockerfile, tag));
            i = j;
        } else {
            i += 1;
        }
    }
    builds
}

/// The image references `docker run … <image>` names in a workflow (the token after
/// `docker run`, skipping flags and their arguments up to the first bare token). Kept
/// deliberately simple: it recognises the flags THIS workflow uses (`--rm`,
/// `--entrypoint <x>`).
fn docker_run_images(workflow: &str) -> Vec<String> {
    let mut images = Vec::new();
    let toks: Vec<&str> = workflow.split_whitespace().collect();
    let mut i = 0;
    while i + 1 < toks.len() {
        if toks[i] == "docker" && toks[i + 1] == "run" {
            let mut j = i + 2;
            while j < toks.len() {
                match toks[j] {
                    "--rm" => j += 1,
                    "--entrypoint" | "--network" | "-e" | "--build-arg" => j += 2,
                    other => {
                        images.push(other.to_string());
                        break;
                    }
                }
            }
            i = j.max(i + 2);
        } else {
            i += 1;
        }
    }
    images
}

// ─── (1) Dockerfile shape ─────────────────────────────────────────────────────

#[test]
fn dockerfile_is_multistage_nonroot_and_parameterized() {
    let df = read(DOCKERFILE);

    let from_count = df
        .lines()
        .filter(|l| l.trim_start().starts_with("FROM "))
        .count();
    assert!(
        from_count >= 2,
        "{DOCKERFILE} is not multi-stage: found {from_count} FROM line(s), want >= 2"
    );
    assert!(
        df.contains("COPY --from=build"),
        "{DOCKERFILE} runtime stage does not copy the binary from the build stage"
    );

    // A non-root USER must be declared, and it must come before the ENTRYPOINT so it
    // actually applies to the process (the dserver fixture's load-bearing order). Match
    // real instruction lines, not the word appearing in a comment.
    let instruction_line = |directive: &str| -> Option<usize> {
        df.lines()
            .position(|l| l.trim_start().starts_with(&format!("{directive} ")))
    };
    let user_idx = instruction_line("USER")
        .unwrap_or_else(|| panic!("{DOCKERFILE} declares no non-root USER"));
    let entry_idx = df
        .lines()
        .position(|l| l.trim_start().starts_with("ENTRYPOINT"))
        .unwrap_or_else(|| panic!("{DOCKERFILE} declares no ENTRYPOINT"));
    assert!(
        user_idx < entry_idx,
        "{DOCKERFILE} places USER after ENTRYPOINT — the drop-root would not apply"
    );
    assert!(!df.contains("USER root"), "{DOCKERFILE} runs as root");

    assert!(
        df.contains("ARG FEATURES"),
        "{DOCKERFILE} takes no `ARG FEATURES` — #471 cannot adopt the skeleton"
    );
    assert!(
        df.contains("ARG FDB_VERSION"),
        "{DOCKERFILE} takes no `ARG FDB_VERSION` — the client version is not parameterized"
    );
}

// ─── (2) single-source version consistency (load-bearing) ─────────────────────

#[test]
fn fdb_version_is_consistent_across_dockerfile_compose_and_crate() {
    let df = read(DOCKERFILE);
    let compose = read(FDB_COMPOSE);
    let cargo_toml = read("Cargo.toml");

    match check_fdb_version_consistency(&df, &compose, &cargo_toml) {
        Ok(v) => {
            // Sanity: it really is the 7.3 line the crate pins and the compose deploys.
            assert_eq!(
                major_minor(&v),
                "7.3",
                "resolved FDB line `{v}` is not the 7.3 line ADR-0042 deploys"
            );
        }
        Err(why) => panic!("FDB client/cluster version drift: {why}"),
    }
}

// ─── (3) CI workflow ↔ command surface ────────────────────────────────────────

#[test]
fn workflow_exists_resolves_and_filters_the_fdb_surface() {
    let wf = read(WORKFLOW);

    // Brief item 3: "every `cargo xtask <sub>` or `docker` invocation it names resolves".
    // This workflow is a container job that drives `docker` directly (it has no
    // `cargo xtask fdb-image` subcommand, and this bundle adds none — the `xtask/src/`
    // is 439's write-set), so the meaningful, NON-VACUOUS resolution check is over the
    // `docker` invocations. (The earlier `cargo xtask <sub>` loop was vacuous — the
    // workflow names none — so it is dropped in favour of the checks below, which bind.)

    // (a) Every `docker build -f <path>` references a Dockerfile that EXISTS, and the
    //     workflow builds the new production image.
    let builds = docker_builds(&wf);
    assert!(
        !builds.is_empty(),
        "{WORKFLOW} runs no `docker build -f <Dockerfile>` — it builds no image"
    );
    let mut built_tags = Vec::new();
    let mut built_dockerfiles = Vec::new();
    for (dockerfile, tag) in &builds {
        let rel = dockerfile
            .as_ref()
            .unwrap_or_else(|| panic!("{WORKFLOW} has a `docker build` with no `-f <Dockerfile>`"));
        assert!(
            workspace_root().join(rel).exists(),
            "{WORKFLOW} builds `-f {rel}`, which does not exist"
        );
        built_dockerfiles.push(rel.clone());
        if let Some(t) = tag {
            built_tags.push(t.clone());
        }
    }
    assert!(
        built_dockerfiles.iter().any(|p| p == DOCKERFILE),
        "{WORKFLOW} does not build the new production image {DOCKERFILE}"
    );

    // (b) Every `docker run … <image>` runs a tag the workflow actually BUILT — so the
    //     job cannot smoke-test an image it never produced. This is the non-vacuous
    //     replacement for the dropped `cargo xtask` resolution.
    let run_images = docker_run_images(&wf);
    assert!(
        !run_images.is_empty(),
        "{WORKFLOW} never `docker run`s the built image — the build is never exercised"
    );
    for img in &run_images {
        assert!(
            built_tags.iter().any(|t| t == img),
            "{WORKFLOW} runs image `{img}`, which no `docker build -t` in the workflow \
             produced (built tags: {built_tags:?})"
        );
    }

    // (c) The PR path filter must fire on every image build input the required gate does
    //     NOT already cover. `cargo xtask ci` default-compiles core/traits/chunkstore/…,
    //     so those are guarded there; what only THIS job builds is the OFF-by-default
    //     feature surface the image bakes (`--features fdb,etcd`) plus the Docker context.
    //     (Substring is enough — a `paths:` entry is the literal glob string.)
    for entry in [
        "crates/metadata-fdb/**",      // the fdb backend + its client version pin
        "deploy/docker/wyrd/**",       // the image home
        "crates/server/**",            // the baked bin: its fdb/etcd cfg arms + usage smoke
        "crates/coordination-etcd/**", // the etcd feature tree — no other CI builds it
        ".dockerignore",               // decides what `COPY . .` ships into the build
    ] {
        assert!(
            wf.contains(entry),
            "{WORKFLOW} path filter omits `{entry}` — a change there can break the \
             `fdb,etcd` image with no other CI job catching it"
        );
    }
}

// ─── (4) demonstrated red — the consistency check is load-bearing ─────────────

#[test]
fn consistency_check_is_red_on_a_mismatched_fdb_version() {
    // Plant a temp-fixture Dockerfile whose FDB_VERSION disagrees with the real cluster
    // tag and crate line, and prove the SAME `check_fdb_version_consistency` the green
    // test drives rejects it — a demonstrated red, not a check resting on file
    // non-existence (the `deploy_no_orchestrator_coupling.rs:67` planted-red pattern).
    // pid + per-process counter for uniqueness — no wall-clock read (#619).
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "wyrd-fdb-image-fixture-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("create fixture dir");
    let bad_dockerfile = "FROM rust:1.96.0-bookworm AS build\n\
         ARG FEATURES=\"\"\n\
         ARG FDB_VERSION=7.1.99\n\
         FROM debian:bookworm-slim\n\
         USER wyrd:wyrd\n\
         ENTRYPOINT [\"wyrd\"]\n";
    let df_path = dir.join("Dockerfile");
    std::fs::write(&df_path, bad_dockerfile).expect("write fixture Dockerfile");

    // Real compose + Cargo.toml, so only the planted mismatch differs.
    let compose = read(FDB_COMPOSE);
    let cargo_toml = read("Cargo.toml");
    let planted = std::fs::read_to_string(&df_path).expect("read fixture Dockerfile");
    let result = check_fdb_version_consistency(&planted, &compose, &cargo_toml);
    std::fs::remove_dir_all(&dir).ok();

    let err = result.expect_err(
        "consistency check passed a Dockerfile pinning FDB_VERSION=7.1.99 against a \
         7.3.77 cluster — the check is vacuous",
    );
    assert!(
        err.contains("7.1.99"),
        "the drift error should name the mismatched version, got: {err}"
    );
}
