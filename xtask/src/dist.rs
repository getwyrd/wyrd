//! `cargo xtask dist` — the distribution-packaging pipeline (#570; ADR-0010's ship
//! order: binary primary, OCI image the same binary).
//!
//! One build serves both artifacts: the pipeline drives `docker build` on the
//! production Dockerfile (`deploy/docker/wyrd/Dockerfile`, #470, unchanged) and then
//! **extracts the binary out of the image** (`docker create` + `docker cp`), so the
//! tarball's `bin/wyrd` is bit-identical to the image's `/usr/local/bin/wyrd`, carries
//! the image's glibc floor (Debian bookworm, 2.36), and the packaging host needs none
//! of the fdb/etcd build toolchain (`libfdb_c`, `protoc`, `cmake`, `clang`). The
//! `--host` fallback builds with the host `cargo` instead, for developers who have the
//! toolchain and want a quick host-glibc tarball.
//!
//! The tarball's operator-facing content — systemd units, env examples, `install.sh`,
//! README — are REAL FILES under `deploy/dist/` (reviewable as content, greppable by
//! `xtask/tests/dist_templates.rs` exactly as `fdb_image.rs` greps the Dockerfile),
//! not strings embedded here. This module only stages them, substituting the
//! `@VERSION@` / `@FDB_VERSION@` tokens; the units' `@BINDIR@` token is deliberately
//! NOT staged away — `install.sh` substitutes it at install time so a custom
//! `--prefix` can never leave `ExecStart` pointing at a nonexistent binary.
//!
//! `FDB_VERSION` has a single source of truth: the Dockerfile's `ARG FDB_VERSION=`,
//! the same pin `xtask/tests/fdb_image.rs` couples to the compose fixture and the
//! crate feature. This module re-parses it (same shape) so `install.sh`'s printed
//! `foundationdb-clients` remediation can never drift from the image.
//!
//! Deliberately NOT part of `cargo xtask ci` (needs docker + network, like
//! `integration`); the pure decisions below (arg parsing, version normalization,
//! staging plan, placeholder policy) are unit-tested inside `ci`.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The tikv flavor is deliberately not offered: TiKV development is stood down
/// (#443) and `tikv-client` carries an unpatched advisory the dependency wall
/// quarantines (`deny-all-features.toml`, RUSTSEC-2026-0104, #543) — a shipped
/// artifact must not link it. Slot documented in `.github/workflows/release.yml`.
pub const DEFAULT_FEATURES: &str = "fdb,etcd";
/// Artifact flavor suffix for the default feature set (`wyrd:<version>-fdb`).
pub const DEFAULT_FLAVOR: &str = "fdb";
/// Where the production image build puts the binary (`deploy/docker/wyrd/Dockerfile`).
pub const IMAGE_BINARY_PATH: &str = "/usr/local/bin/wyrd";
/// The single platform the pipeline builds and advertises: the tarball name says
/// `x86_64-unknown-linux-gnu`, so the image build is pinned to linux/amd64 rather
/// than following the host default (an arm64 host would otherwise ship an aarch64
/// binary under an x86_64 filename). Multi-arch is a follow-up slice.
pub const DIST_PLATFORM: &str = "linux/amd64";
/// The staging/output root, under the workspace `target/` (never committed).
pub const DIST_DIR: &str = "target/dist";

/// The parsed `cargo xtask dist` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistConfig {
    /// Validate templates + placeholders only; build nothing.
    pub check_only: bool,
    /// Keep the built image tagged (`wyrd:<version>-<flavor>`, `wyrd:<flavor>`).
    pub image: bool,
    /// Additionally `docker save` the image into `target/dist/`.
    pub oci_archive: bool,
    /// Build with the host `cargo` instead of extracting from the image.
    pub host_build: bool,
    /// Cargo features baked into the binary/image.
    pub features: String,
    /// Artifact flavor suffix.
    pub flavor: String,
}

impl Default for DistConfig {
    fn default() -> Self {
        Self {
            check_only: false,
            image: false,
            oci_archive: false,
            host_build: false,
            features: DEFAULT_FEATURES.to_string(),
            flavor: DEFAULT_FLAVOR.to_string(),
        }
    }
}

/// Parse `cargo xtask dist` arguments. Pure — unit-tested in `ci`.
pub fn parse_args(args: &[String]) -> Result<DistConfig, String> {
    let mut cfg = DistConfig::default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--check" => cfg.check_only = true,
            "--image" => cfg.image = true,
            "--oci-archive" => {
                cfg.image = true;
                cfg.oci_archive = true;
            }
            "--host" => cfg.host_build = true,
            "--features" => {
                cfg.features = it
                    .next()
                    .ok_or_else(|| "dist: --features needs a value".to_string())?
                    .clone();
            }
            "--flavor" => {
                cfg.flavor = it
                    .next()
                    .ok_or_else(|| "dist: --flavor needs a value".to_string())?
                    .clone();
            }
            other => {
                return Err(format!(
                    "dist: unknown argument `{other}` (known: --check --image --oci-archive \
                     --host --features <list> --flavor <name>)"
                ));
            }
        }
    }
    if cfg.host_build && cfg.image {
        return Err("dist: --host builds no image; drop --image/--oci-archive or --host".into());
    }
    Ok(cfg)
}

/// Normalize `git describe --tags --always --dirty` output into an artifact version.
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

/// The artifact version rendered as a DOCKER TAG: a tag may not contain `+`
/// (`[a-zA-Z0-9_.-]` only), so the semver build-metadata separator becomes `-`.
/// Filenames keep the `+` — only image tags are constrained. Pure — unit-tested
/// in `ci`.
pub fn image_tag_version(version: &str) -> String {
    version.replace('+', "-")
}

/// The default value of `ARG FDB_VERSION=<v>` in the production Dockerfile — the
/// single source of truth for the client-package pin, the same parse
/// `xtask/tests/fdb_image.rs` uses. Pure — unit-tested in `ci`.
pub fn dockerfile_fdb_version(dockerfile: &str) -> Option<String> {
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

/// One file the tarball stages: repo-relative source, staging-relative destination,
/// executable bit, and whether the `@VERSION@`/`@FDB_VERSION@` tokens are substituted
/// at staging time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    pub source: &'static str,
    pub dest: &'static str,
    pub executable: bool,
    pub substitute: bool,
}

/// The tarball layout (everything except `bin/wyrd` and the generated `VERSION`
/// file, which the runner adds). Pure — the layout test pins every row.
///
/// The systemd units are staged VERBATIM (`substitute: false`): their `@BINDIR@`
/// token belongs to `install.sh`'s install-time substitution, not to staging.
pub fn staging_plan() -> Vec<StagedFile> {
    vec![
        StagedFile {
            source: "deploy/dist/install.sh",
            dest: "install.sh",
            executable: true,
            substitute: true,
        },
        StagedFile {
            source: "deploy/dist/README.md",
            dest: "README.md",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "deploy/dist/systemd/wyrd-d-server.service",
            dest: "systemd/wyrd-d-server.service",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "deploy/dist/systemd/wyrd-custodian.service",
            dest: "systemd/wyrd-custodian.service",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "deploy/dist/systemd/wyrd-s3.service",
            dest: "systemd/wyrd-s3.service",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "deploy/dist/env/d-server.env.example",
            dest: "etc/d-server.env.example",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "deploy/dist/env/custodian.env.example",
            dest: "etc/custodian.env.example",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "deploy/dist/env/s3.env.example",
            dest: "etc/s3.env.example",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "LICENSE",
            dest: "LICENSE",
            executable: false,
            substitute: false,
        },
        StagedFile {
            source: "NOTICE",
            dest: "NOTICE",
            executable: false,
            substitute: false,
        },
    ]
}

/// The one token that legitimately SURVIVES staging: `@BINDIR@` is substituted by
/// `install.sh` at install time (it is both the units' `ExecStart` token and the
/// sed pattern inside install.sh itself), never by this pipeline.
pub const INSTALL_TIME_TOKENS: [&str; 1] = ["@BINDIR@"];

/// Substitute the staging-time tokens into `content`, refusing any that survive: a
/// leftover `@LIKE_THIS@` token in a substituted file is template drift (a new token
/// nobody wired), and shipping it would print literally in an operator's terminal.
/// [`INSTALL_TIME_TOKENS`] are exempt — they are wired, just at a later stage.
/// Pure — unit-tested in `ci`.
pub fn substitute_tokens(
    content: &str,
    version: &str,
    fdb_version: &str,
) -> Result<String, String> {
    let out = content
        .replace("@VERSION@", version)
        .replace("@FDB_VERSION@", fdb_version);
    let mut rest = out.as_str();
    while let Some(token) = find_placeholder(rest) {
        if !INSTALL_TIME_TOKENS.contains(&token.as_str()) {
            return Err(format!(
                "dist: unsubstituted placeholder `{token}` after staging substitution — wire it \
                 in xtask/src/dist.rs or remove it from the template"
            ));
        }
        let after = rest.find(&token).expect("token was just found") + token.len();
        rest = &rest[after..];
    }
    Ok(out)
}

/// The first `@UPPER_CASE@` token in `content`, if any. Pure helper for the
/// leftover-placeholder refusal above (and for the tests that pin the templates
/// carry the tokens they must).
pub fn find_placeholder(content: &str) -> Option<String> {
    let bytes = content.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'@' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && (bytes[end].is_ascii_uppercase() || bytes[end] == b'_') {
                end += 1;
            }
            if end > start && end < bytes.len() && bytes[end] == b'@' {
                return Some(content[i..=end].to_string());
            }
        }
        i += 1;
    }
    None
}

// ─── imperative runner ──────────────────────────────────────────────────────────

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask is nested under the workspace root")
        .to_path_buf()
}

fn read(root: &Path, rel: &str) -> Result<String, String> {
    let path = root.join(rel);
    std::fs::read_to_string(&path).map_err(|e| format!("dist: read {}: {e}", path.display()))
}

/// Capture a command's stdout, failing loudly on a non-zero exit.
fn capture(cmd: &mut Command, what: &str) -> Result<String, String> {
    let out = cmd
        .output()
        .map_err(|e| format!("dist: spawn {what}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "dist: {what} failed ({}):\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Run a command inheriting stdio (build steps whose progress the operator watches).
fn run(cmd: &mut Command, what: &str) -> Result<(), String> {
    let status = cmd
        .status()
        .map_err(|e| format!("dist: spawn {what}: {e}"))?;
    if !status.success() {
        return Err(format!("dist: {what} failed ({status})"));
    }
    Ok(())
}

/// The artifact version for this checkout (see [`normalize_describe`]).
fn derive_version(root: &Path) -> Result<String, String> {
    let describe = capture(
        Command::new("git")
            .args(["describe", "--tags", "--always", "--dirty"])
            .current_dir(root),
        "git describe",
    )?;
    Ok(normalize_describe(&describe, "0.0.0"))
}

/// The host target triple (`rustc -vV`'s `host:` line) — names the tarball honestly;
/// the docker-extracted binary is linux-gnu by construction.
fn host_triple() -> Result<String, String> {
    let out = capture(Command::new("rustc").arg("-vV"), "rustc -vV")?;
    out.lines()
        .find_map(|l| l.strip_prefix("host: "))
        .map(|s| s.trim().to_string())
        .ok_or_else(|| "dist: rustc -vV printed no `host:` line".into())
}

/// Validate every template exists, the substitution tokens are present where they
/// must be, and no unknown placeholder lurks — the `--check` mode, and the first
/// step of every full run.
fn check_templates(root: &Path, version: &str, fdb_version: &str) -> Result<(), String> {
    for file in staging_plan() {
        let content = read(root, file.source)?;
        if file.substitute {
            substitute_tokens(&content, version, fdb_version)?;
        }
    }
    let install = read(root, "deploy/dist/install.sh")?;
    for required in ["@VERSION@", "@FDB_VERSION@"] {
        if !install.contains(required) {
            return Err(format!(
                "dist: deploy/dist/install.sh lost its {required} token — the shipped installer \
                 would print a stale value"
            ));
        }
    }
    for unit in [
        "deploy/dist/systemd/wyrd-d-server.service",
        "deploy/dist/systemd/wyrd-custodian.service",
        "deploy/dist/systemd/wyrd-s3.service",
    ] {
        let content = read(root, unit)?;
        if !content.contains("ExecStart=\"@BINDIR@/wyrd\"") {
            return Err(format!(
                "dist: {unit} must keep `ExecStart=\"@BINDIR@/wyrd\" …` — install.sh substitutes \
                 @BINDIR@ at install time (quoted, so a whitespace prefix stays one word) and a \
                 --prefix install stays runnable"
            ));
        }
    }
    Ok(())
}

/// Build the image and extract `bin/wyrd` from it (the default path), or `--host`
/// build with the local cargo.
fn obtain_binary(root: &Path, cfg: &DistConfig, version: &str) -> Result<PathBuf, String> {
    if cfg.host_build {
        run(
            Command::new("cargo")
                .args([
                    "build",
                    "--release",
                    "--locked",
                    "--bin",
                    "wyrd",
                    "--features",
                    &cfg.features,
                ])
                .current_dir(root),
            "cargo build --release",
        )?;
        return Ok(root.join("target/release/wyrd"));
    }

    let sha = capture(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(root),
        "git rev-parse HEAD",
    )?;
    let versioned_tag = format!(
        "wyrd:{tag}-{flavor}",
        tag = image_tag_version(version),
        flavor = cfg.flavor
    );
    let flavor_tag = format!("wyrd:{flavor}", flavor = cfg.flavor);
    // `--platform linux/amd64`, PINNED: the tarball advertises
    // `x86_64-unknown-linux-gnu` in its name, so the build must not silently
    // follow an arm64 host's default platform and ship an aarch64 binary under
    // an x86_64 filename (codex P1). An arm64 packaging host needs binfmt/qemu
    // for this — a slow but honest build; release CI is amd64 anyway.
    let mut args: Vec<String> = [
        "buildx",
        "build",
        "--platform",
        DIST_PLATFORM,
        "--build-arg",
        &format!("FEATURES={}", cfg.features),
        "--label",
        &format!("org.opencontainers.image.version={version}"),
        "--label",
        &format!("org.opencontainers.image.revision={}", sha.trim()),
        "-t",
        &versioned_tag,
        "-t",
        &flavor_tag,
        // The docker exporter loads the tagged image into the daemon (what the
        // classic `docker build` did) — spelled explicitly because the OCI
        // exporter below may join it.
        "--output",
        "type=docker",
    ]
    .map(str::to_string)
    .to_vec();
    if cfg.oci_archive {
        // The OCI archive is a SECOND EXPORTER on the SAME build (buildx ≥0.13
        // multi-output), never a separate build: a cache miss or any
        // nondeterministic input in a re-build could otherwise ship an archive
        // whose binary differs from the tarball's, silently breaking the
        // "identical binary" guarantee (codex P1). `docker save` is no
        // alternative — its output is Docker's legacy archive format, which
        // OCI-only consumers (`skopeo copy oci-archive:…`) reject.
        std::fs::create_dir_all(root.join(DIST_DIR))
            .map_err(|e| format!("dist: mkdir {DIST_DIR}: {e}"))?;
        args.push("--output".into());
        args.push(format!(
            "type=oci,dest={DIST_DIR}/{}",
            oci_archive_name(version, &cfg.flavor)
        ));
    }
    args.extend(["-f", "deploy/docker/wyrd/Dockerfile", "."].map(str::to_string));
    run(
        Command::new("docker").args(&args).current_dir(root),
        "docker buildx build",
    )?;

    let cid = capture(
        Command::new("docker").args(["create", "--platform", DIST_PLATFORM, &versioned_tag]),
        "docker create",
    )?;
    let cid = cid.trim().to_string();
    let extracted = root.join(DIST_DIR).join("wyrd.extracted");
    std::fs::create_dir_all(root.join(DIST_DIR))
        .map_err(|e| format!("dist: mkdir {DIST_DIR}: {e}"))?;
    let cp = run(
        Command::new("docker").args([
            "cp",
            &format!("{cid}:{IMAGE_BINARY_PATH}"),
            &extracted.to_string_lossy(),
        ]),
        "docker cp (extract binary)",
    );
    // Remove the throwaway container regardless of the cp outcome.
    let _ = Command::new("docker").args(["rm", "-f", &cid]).output();
    cp?;
    Ok(extracted)
}

/// Assemble the staging tree and tar it, returning the tarball's file name
/// (relative to [`DIST_DIR`]); checksums and the optional OCI export follow in
/// [`run_dist`], which needs the full artifact list.
fn assemble(
    root: &Path,
    cfg: &DistConfig,
    version: &str,
    fdb_version: &str,
    binary: &Path,
) -> Result<String, String> {
    use std::os::unix::fs::PermissionsExt;

    let triple = if cfg.host_build {
        host_triple()?
    } else {
        // The image build is linux/amd64 bookworm regardless of the host toolchain.
        "x86_64-unknown-linux-gnu".to_string()
    };
    let top = format!("wyrd-{version}-{triple}");
    let stage = root.join(DIST_DIR).join(&top);
    if stage.exists() {
        std::fs::remove_dir_all(&stage).map_err(|e| format!("dist: clean staging: {e}"))?;
    }

    for file in staging_plan() {
        let content = read(root, file.source)?;
        let content = if file.substitute {
            substitute_tokens(&content, version, fdb_version)?
        } else {
            content
        };
        let dest = stage.join(file.dest);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("dist: mkdir: {e}"))?;
        }
        std::fs::write(&dest, content)
            .map_err(|e| format!("dist: write {}: {e}", dest.display()))?;
        let mode = if file.executable { 0o755 } else { 0o644 };
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(mode))
            .map_err(|e| format!("dist: chmod {}: {e}", dest.display()))?;
    }

    let bin_dest = stage.join("bin/wyrd");
    std::fs::create_dir_all(stage.join("bin")).map_err(|e| format!("dist: mkdir bin: {e}"))?;
    std::fs::copy(binary, &bin_dest)
        .map_err(|e| format!("dist: copy binary {}: {e}", binary.display()))?;
    std::fs::set_permissions(&bin_dest, std::fs::Permissions::from_mode(0o755))
        .map_err(|e| format!("dist: chmod bin/wyrd: {e}"))?;

    let version_file = format!(
        "version: {version}\nflavor: {flavor}\nfoundationdb-clients: {fdb_version}\n",
        flavor = cfg.flavor
    );
    std::fs::write(stage.join("VERSION"), version_file)
        .map_err(|e| format!("dist: write VERSION: {e}"))?;

    let tarball = format!("{top}.tar.gz");
    run(
        Command::new("tar")
            .args(["czf", &tarball, &top])
            .current_dir(root.join(DIST_DIR)),
        "tar czf",
    )?;
    println!("\nxtask dist: staged {top}");
    Ok(tarball)
}

/// The OCI archive's file name (pre-gzip) for a version/flavor pair — one place,
/// because the build step writes it and `run_dist` gzips and checksums it.
fn oci_archive_name(version: &str, flavor: &str) -> String {
    format!("wyrd-{version}-{flavor}.oci.tar")
}

/// Checksum the produced artifacts into `SHA256SUMS` (what the release signs and
/// operators verify against).
fn write_checksums(root: &Path, artifacts: &[String]) -> Result<(), String> {
    let sums = capture(
        Command::new("sha256sum")
            .args(artifacts)
            .current_dir(root.join(DIST_DIR)),
        "sha256sum",
    )?;
    std::fs::write(root.join(DIST_DIR).join("SHA256SUMS"), sums)
        .map_err(|e| format!("dist: write SHA256SUMS: {e}"))
}

/// The `cargo xtask dist` entry point.
pub fn run_dist(args: &[String]) -> Result<(), String> {
    let cfg = parse_args(args)?;
    let root = workspace_root();

    let dockerfile = read(&root, "deploy/docker/wyrd/Dockerfile")?;
    let fdb_version = dockerfile_fdb_version(&dockerfile).ok_or_else(|| {
        "dist: deploy/docker/wyrd/Dockerfile declares no `ARG FDB_VERSION=` — the \
         install.sh remediation pin has lost its source of truth"
            .to_string()
    })?;
    let version = derive_version(&root)?;

    check_templates(&root, &version, &fdb_version)?;
    if cfg.check_only {
        println!("xtask dist --check: templates, tokens, and pins are consistent (version would be {version})");
        return Ok(());
    }

    let binary = obtain_binary(&root, &cfg, &version)?;
    let tarball = assemble(&root, &cfg, &version, &fdb_version, &binary)?;

    let mut artifacts = vec![tarball.clone()];
    if cfg.oci_archive {
        // The archive was written by the SAME build that produced the extracted
        // binary (a second exporter on one build, `obtain_binary`); this step
        // only compresses it.
        let archive = oci_archive_name(&version, &cfg.flavor);
        run(
            Command::new("gzip")
                .args(["-f", &archive])
                .current_dir(root.join(DIST_DIR)),
            "gzip oci archive",
        )?;
        artifacts.push(format!("{archive}.gz"));
    }
    write_checksums(&root, &artifacts)?;

    let versioned_tag = format!(
        "wyrd:{tag}-{flavor}",
        tag = image_tag_version(&version),
        flavor = cfg.flavor
    );
    let flavor_tag = format!("wyrd:{flavor}", flavor = cfg.flavor);
    if cfg.image {
        println!("xtask dist: image {versioned_tag} (also {flavor_tag})");
    } else if !cfg.host_build {
        // The image was only the build vehicle: honour DistConfig::image's
        // contract and do not accumulate multi-hundred-MB tags run after run
        // (codex P2). Failure to untag is not failure to dist.
        let _ = Command::new("docker")
            .args(["rmi", &versioned_tag, &flavor_tag])
            .current_dir(&root)
            .output();
    }
    println!("xtask dist: wrote {DIST_DIR}/{tarball} (+ SHA256SUMS)");
    Ok(())
}
