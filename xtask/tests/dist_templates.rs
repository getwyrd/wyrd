//! Regression guards for the distribution package (#570): the `deploy/dist/`
//! templates `cargo xtask dist` stages into the operator tarball, and the pure
//! packaging decisions in `xtask::dist`.
//!
//! Container-free by design (ADR-0016), following `xtask/tests/fdb_image.rs`:
//! every template assertion is a file read + substring check, so the shape an
//! operator installs is pinned inside `cargo xtask ci` while the actual build
//! (`docker build`, tar) is the deferred half exercised by
//! `.github/workflows/release.yml`.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use xtask::dist;

// The two-binary layout contract's checker and local expected set (#742), included as a
// MODULE rather than imported from a sibling helper: that file is the red-earning text
// test (it names no `xtask::dist` symbol, so it compiles — and fails — against a reverted
// tree), and a helper under `xtask/tests/` would be classified as a test of its own. One
// checker, run twice: there over its local copy of the set, here over the production table.
#[path = "dist_two_binary_layout.rs"]
mod layout;

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

const UNITS: [(&str, &str, &str); 3] = [
    (
        "deploy/dist/systemd/wyrd-d-server.service",
        "d-server",
        "WYRD_D_SERVER_ARGS",
    ),
    (
        "deploy/dist/systemd/wyrd-custodian.service",
        "custodian",
        "WYRD_CUSTODIAN_ARGS",
    ),
    ("deploy/dist/systemd/wyrd-s3.service", "s3", "WYRD_S3_ARGS"),
];

// ─── systemd units ──────────────────────────────────────────────────────────────

/// Every unit runs unprivileged, takes its operator intent from /etc/wyrd, and
/// keeps the install-time `@BINDIR@` token — the seam that makes a custom
/// `--prefix` install runnable (install.sh substitutes it; a hardcoded path here
/// would break every non-default prefix, the codex P1 on the plan).
#[test]
fn units_run_unprivileged_from_etc_wyrd_and_keep_the_bindir_token() {
    for (path, role, args_var) in UNITS {
        let unit = read(path);
        for required in [
            "User=wyrd",
            "Group=wyrd",
            &format!("EnvironmentFile=/etc/wyrd/{role}.env") as &str,
            // The binary word is double-quoted: a custom --prefix containing
            // whitespace must stay ONE systemd word after install-time
            // substitution.
            &format!(
                "ExecStart=\"@BINDIR@/wyrd\" {role} --data-dir ${{STATE_DIRECTORY}} ${args_var}"
            ),
            "Restart=on-failure",
            "[Install]",
            "WantedBy=multi-user.target",
        ] {
            assert!(
                unit.contains(required),
                "{path} must contain `{required}` — the operator contract the tarball ships"
            );
        }
    }
}

/// The hardening baseline: dropping any of these silently widens every deployed
/// host's attack surface. (MemoryDenyWriteExecute is deliberately absent in v1 —
/// the unit comments say why.)
#[test]
fn units_carry_the_hardening_baseline() {
    for (path, _, _) in UNITS {
        let unit = read(path);
        for directive in [
            "NoNewPrivileges=yes",
            "ProtectSystem=strict",
            "ProtectHome=yes",
            "PrivateTmp=yes",
            "RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX",
            "SystemCallFilter=@system-service",
            "CapabilityBoundingSet=",
        ] {
            assert!(
                unit.contains(directive),
                "{path} lost hardening directive `{directive}`"
            );
        }
    }
}

/// The FDB client REWRITES the cluster file when coordinators change; under
/// ProtectSystem=strict the metadata-opening roles need the explicit grant (with
/// the `-` prefix so redb-only hosts stay valid). The d-server never opens
/// metadata, so it must NOT carry the grant — least privilege.
#[test]
fn only_the_metadata_opening_roles_may_write_the_fdb_cluster_file() {
    const GRANT: &str = "ReadWritePaths=-/etc/foundationdb";
    assert!(read("deploy/dist/systemd/wyrd-custodian.service").contains(GRANT));
    assert!(read("deploy/dist/systemd/wyrd-s3.service").contains(GRANT));
    assert!(
        !read("deploy/dist/systemd/wyrd-d-server.service").contains("foundationdb"),
        "the d-server opens no metadata store — granting it the cluster-file write \
         would be gratuitous privilege"
    );
}

/// The custodian unit must carry the single-active warning: the blueprint's
/// loudest deployment rule, and the one a unit file could silently invite
/// operators to violate by being enabled on two hosts.
#[test]
fn the_custodian_unit_warns_run_exactly_one() {
    let unit = read("deploy/dist/systemd/wyrd-custodian.service");
    assert!(
        unit.contains("exactly ONE") && unit.contains("#365"),
        "wyrd-custodian.service lost the run-exactly-one warning (single-active is \
         not enforced until the etcd Coordination backend, #365)"
    );
}

// ─── env examples ───────────────────────────────────────────────────────────────

/// Each env example must name every load-bearing flag of its role's blueprint
/// invocation — an operator fills in values, never discovers flags.
#[test]
fn env_examples_name_every_load_bearing_flag() {
    let d = read("deploy/dist/env/d-server.env.example");
    for flag in [
        "--bind",
        "--advertise-addr",
        "--id",
        "--failure-domain",
        "--coordination-backend etcd",
        "--group",
        "WYRD_ETCD_ENDPOINTS",
    ] {
        assert!(d.contains(flag), "d-server.env.example lost `{flag}`");
    }

    let c = read("deploy/dist/env/custodian.env.example");
    for flag in [
        // The CLI defaults to dev-only redb: without the explicit backend the
        // custodian would reconcile a DIFFERENT metadata store than the
        // gateways (codex P1 on the plan).
        "--metadata-backend fdb",
        "--zone",
        "--endpoints",
        "--ids",
        "--failure-domains",
        "--otlp-endpoint",
        "WYRD_FDB_CLUSTER_FILE",
        "exactly ONE",
    ] {
        assert!(c.contains(flag), "custodian.env.example lost `{flag}`");
    }
    // The custodian takes NO --coordination-backend (it campaigns through
    // process-local coordination only — the very reason run-exactly-one is on
    // the operator, #365). Shipping the flag would be silently ignored and
    // imply a fencing that is not active.
    assert!(
        !c.contains(
            "WYRD_CUSTODIAN_ARGS=--zone zone-a --metadata-backend fdb --coordination-backend"
        ) && !c.contains("WYRD_ETCD_ENDPOINTS="),
        "custodian.env.example must not configure a coordination backend the role does not consume"
    );

    let s = read("deploy/dist/env/s3.env.example");
    for flag in [
        "--metadata-backend fdb",
        "--coordination-backend etcd",
        "--s3-listen",
        "--region",
        "--endpoints",
        "WYRD_FDB_CLUSTER_FILE",
        "WYRD_ETCD_ENDPOINTS",
        "WYRD_S3_ACCESS_KEY",
        "WYRD_S3_SECRET_KEY",
    ] {
        assert!(s.contains(flag), "s3.env.example lost `{flag}`");
    }
    // `--chunk-size` must be on the LIVE `WYRD_S3_ARGS=` line, not just somewhere in the
    // file: its comment block names the flag too, so a whole-file `contains` would still
    // pass with the flag deleted from the args the unit actually runs (#738). Its value must
    // be one the role accepts (1048576..=16777216), or the shipped template would not start.
    let s3_args: Vec<&str> = s
        .lines()
        .filter_map(|line| line.strip_prefix("WYRD_S3_ARGS="))
        .collect();
    assert_eq!(
        s3_args.len(),
        1,
        "s3.env.example must carry exactly one live `WYRD_S3_ARGS=` line"
    );
    let args: Vec<&str> = s3_args[0].split_whitespace().collect();
    let chunk_size = args
        .windows(2)
        .find(|pair| pair[0] == "--chunk-size")
        .map(|pair| pair[1])
        .unwrap_or_else(|| {
            panic!("s3.env.example's `WYRD_S3_ARGS=` line lost `--chunk-size N`: {args:?}")
        });
    assert!(
        chunk_size
            .parse::<usize>()
            .is_ok_and(|n| (1_048_576..=16_777_216).contains(&n)),
        "s3.env.example's `--chunk-size {chunk_size}` is outside what `wyrd s3` accepts (1048576..=16777216)"
    );
    // The credential assignments must ship COMMENTED OUT: the CLI checks the
    // variables for PRESENCE, so an empty-but-set `WYRD_S3_ACCESS_KEY=` would
    // start the gateway with empty-string credentials instead of refusing —
    // the fail-closed contract would be silently voided by the template itself.
    for line in s.lines() {
        assert!(
            !line.starts_with("WYRD_S3_ACCESS_KEY") && !line.starts_with("WYRD_S3_SECRET_KEY"),
            "s3.env.example must not ship an ACTIVE credential assignment \
             (present-but-empty passes the CLI's presence check): `{line}`"
        );
    }
}

// ─── install.sh ─────────────────────────────────────────────────────────────────

/// The installer's non-negotiables: strict shell, the staging-time tokens, the
/// install-time @BINDIR@ substitution, daemon-reload, and an uninstall path.
#[test]
fn install_sh_keeps_its_contract() {
    let sh = read("deploy/dist/install.sh");
    assert!(sh.starts_with("#!/bin/sh"), "install.sh must be POSIX sh");
    for required in [
        "set -eu",
        "@VERSION@",
        "@FDB_VERSION@",
        // The prefix is operator input crossing TWO parsers. systemd: `%` is a
        // specifier (doubled), the quoted ExecStart word makes whitespace safe,
        // and an unrepresentable double quote / newline is refused. sed: escape
        // \ & and the | delimiter (an unescaped `&` expands back to `@BINDIR@`)
        // — substituting the raw $BINDIR would install broken units for legal
        // paths.
        "tr -d '\\n\"$' | tr -d \"\\\\\\\\\"",
        // A /home-, /root-, or /run/user-prefixed binary would be hidden from
        // the services by their ProtectHome=yes hardening — install.sh refuses
        // such prefixes up front instead of installing units that cannot start.
        "/home/* | /home | /root/* | /root | /run/user/*",
        "BINDIR_UNIT=$(printf '%s' \"$BINDIR\" | sed 's/%/%%/g')",
        "BINDIR_ESCAPED=$(printf '%s' \"$BINDIR_UNIT\" | sed 's/[\\\\&|]/\\\\&/g')",
        "s|@BINDIR@|$BINDIR_ESCAPED|g",
        "systemctl daemon-reload",
        "--uninstall",
        "foundationdb-clients_@FDB_VERSION@-1_amd64.deb",
        // A custom-prefix install must be uninstallable without re-typing the
        // prefix: the install RECORDS it, the uninstall READS it (explicit
        // --prefix overriding), else `--uninstall` would silently aim at
        // /usr/local and keep the real binary.
        "printf '%s\\n' \"$PREFIX\" >\"$CONFDIR/install-prefix\"",
        "PREFIX=$(cat \"$CONFDIR/install-prefix\")",
    ] {
        assert!(sh.contains(required), "install.sh lost `{required}`");
    }
}

/// The installer must never enable or start a role — wiring a host into a cluster
/// is the operator's decision (and auto-starting the custodian could violate
/// run-exactly-one). `systemctl` in command position may only daemon-reload and
/// (on uninstall) disable.
#[test]
fn install_sh_never_enables_or_starts_units() {
    let sh = read("deploy/dist/install.sh");
    for line in sh.lines() {
        let t = line.trim_start();
        if let Some(rest) = t.strip_prefix("systemctl ") {
            assert!(
                rest.starts_with("daemon-reload") || rest.starts_with("disable"),
                "install.sh executes `systemctl {rest}` — only daemon-reload and \
                 disable (uninstall) are allowed; enabling/starting is the operator's call"
            );
        }
    }
}

/// The live config is the operator's: an upgrade must never overwrite an existing
/// /etc/wyrd/<role>.env (only the .example is refreshed).
#[test]
fn install_sh_preserves_live_configs_on_upgrade() {
    let sh = read("deploy/dist/install.sh");
    assert!(
        sh.contains("if [ ! -f \"$CONFDIR/$role.env\" ]"),
        "install.sh must copy the live <role>.env only when absent — upgrades \
         never clobber operator config"
    );
}

// ─── the FDB client-version pin, fourth surface ─────────────────────────────────

/// `install.sh`'s printed foundationdb-clients remediation is substituted from the
/// Dockerfile's `ARG FDB_VERSION` — the SAME pin `fdb_image.rs` couples to the
/// compose fixture and the crate feature. This pins the parser agreement, so the
/// installer can never tell an operator to install a client that mismatches the
/// image/cluster.
#[test]
fn the_installer_pin_shares_the_dockerfile_source_of_truth() {
    let dockerfile = read("deploy/docker/wyrd/Dockerfile");
    let version = dist::dockerfile_fdb_version(&dockerfile)
        .expect("deploy/docker/wyrd/Dockerfile declares ARG FDB_VERSION=<v>");
    assert!(
        version.split('.').count() == 3 && version.chars().all(|c| c.is_ascii_digit() || c == '.'),
        "FDB_VERSION `{version}` is not a full x.y.z client version"
    );
    // And the substitution engine actually consumes the token (end-to-end shape).
    let substituted = dist::substitute_tokens(&read("deploy/dist/install.sh"), "1.2.3", &version)
        .expect("install.sh substitutes cleanly");
    assert!(
        substituted.contains(&format!("foundationdb-clients_{version}-1_amd64.deb")),
        "the substituted install.sh must name foundationdb-clients {version}"
    );
}

// ─── pure packaging decisions ───────────────────────────────────────────────────

/// Version normalization: the three `git describe` shapes an artifact build meets.
#[test]
fn normalize_describe_covers_all_three_shapes() {
    // No tags yet: bare short sha (git describe --always), optionally dirty.
    assert_eq!(
        dist::normalize_describe("abc12de", "0.0.0"),
        "0.0.0+git.abc12de"
    );
    assert_eq!(
        dist::normalize_describe("abc12de-dirty", "0.0.0"),
        "0.0.0+git.abc12de.dirty"
    );
    // Exactly on a tag.
    assert_eq!(dist::normalize_describe("v0.1.0", "0.0.0"), "0.1.0");
    // Past a tag.
    assert_eq!(
        dist::normalize_describe("v0.1.0-3-gabc12de", "0.0.0"),
        "0.1.0+git.3.abc12de"
    );
    assert_eq!(
        dist::normalize_describe("v0.1.0-3-gabc12de-dirty", "0.0.0"),
        "0.1.0+git.3.abc12de.dirty"
    );
}

/// A docker tag may not contain `+` — the semver build-metadata separator must be
/// sanitized in IMAGE TAGS (and only there; filenames keep the `+`).
#[test]
fn image_tags_sanitize_the_semver_plus() {
    assert_eq!(
        dist::image_tag_version("0.0.0+git.abc12de"),
        "0.0.0-git.abc12de"
    );
    assert_eq!(dist::image_tag_version("0.1.0"), "0.1.0");
}

/// The staging plan stages every template that exists and nothing that doesn't:
/// each source is a real repo file, install.sh is the only substituted file and is
/// executable, units stage verbatim (their @BINDIR@ belongs to install time).
#[test]
fn the_staging_plan_matches_the_repo() {
    let plan = dist::staging_plan();
    for file in &plan {
        assert!(
            workspace_root().join(file.source).is_file(),
            "staging plan names a missing source: {}",
            file.source
        );
        if file.source.contains("systemd/") {
            assert!(
                !file.substitute,
                "{} must stage VERBATIM — @BINDIR@ is substituted at install time",
                file.source
            );
        }
    }
    let subs: Vec<_> = plan.iter().filter(|f| f.substitute).collect();
    assert_eq!(
        subs.iter().map(|f| f.source).collect::<Vec<_>>(),
        vec!["deploy/dist/install.sh"],
        "install.sh is the only staging-time-substituted file"
    );
    assert!(subs[0].executable, "install.sh must stage executable");
    // Everything the README promises is in the tarball is actually staged.
    for dest in [
        "install.sh",
        "README.md",
        "LICENSE",
        "NOTICE",
        "systemd/wyrd-d-server.service",
        "systemd/wyrd-custodian.service",
        "systemd/wyrd-s3.service",
        "etc/d-server.env.example",
        "etc/custodian.env.example",
        "etc/s3.env.example",
    ] {
        assert!(
            plan.iter().any(|f| f.dest == dest),
            "the staging plan lost `{dest}`"
        );
    }
}

/// A leftover `@TOKEN@` in a substituted file is template drift and must refuse,
/// while the install-time `@BINDIR@` passes through untouched (it appears in
/// install.sh's own sed expression and in the units' ExecStart).
#[test]
fn leftover_placeholders_refuse_and_bindir_survives() {
    let err = dist::substitute_tokens("hello @NEW_TOKEN@", "1", "2")
        .expect_err("an unwired placeholder must refuse");
    assert!(err.contains("@NEW_TOKEN@"));
    // @BINDIR@ is the wired install-time exemption — even alongside a real
    // substitution, and even repeated.
    let out = dist::substitute_tokens(
        "sed s|@BINDIR@|x| v=@VERSION@ @BINDIR@/wyrd",
        "1.2.3",
        "7.3.77",
    )
    .unwrap();
    assert_eq!(out, "sed s|@BINDIR@|x| v=1.2.3 @BINDIR@/wyrd");
    // ...but an unwired token AFTER a @BINDIR@ still refuses (the scan continues).
    dist::substitute_tokens("@BINDIR@ then @NEW_TOKEN@", "1", "2")
        .expect_err("an unwired placeholder after the exemption must still refuse");
    assert_eq!(dist::find_placeholder("no tokens here"), None);
    assert_eq!(
        dist::find_placeholder("ExecStart=@BINDIR@/wyrd"),
        Some("@BINDIR@".to_string())
    );
    // Substitution replaces both wired tokens.
    let out = dist::substitute_tokens("v=@VERSION@ f=@FDB_VERSION@", "1.2.3", "7.3.77").unwrap();
    assert_eq!(out, "v=1.2.3 f=7.3.77");
}

/// The dist arg parser: --oci-archive implies --image; --host excludes both (a
/// host build produces no image to save); unknown flags refuse.
#[test]
fn dist_args_parse_and_refuse_correctly() {
    let cfg = dist::parse_args(&[]).unwrap();
    assert_eq!(cfg.features, "fdb,etcd");
    assert_eq!(cfg.flavor, "fdb");
    assert!(!cfg.image && !cfg.check_only);

    let cfg = dist::parse_args(&["--oci-archive".into()]).unwrap();
    assert!(cfg.image && cfg.oci_archive);

    dist::parse_args(&["--host".into(), "--image".into()])
        .expect_err("--host builds no image; the pairing must refuse");
    dist::parse_args(&["--bogus".into()]).expect_err("unknown flags must refuse");
}

// ─── the shipped-binary set (#742): declared once, every consumer pinned ────────

/// The production table as the `(in-image path, tarball destination)` pairs the layout
/// checker consumes.
fn production_pairs() -> Vec<(&'static str, &'static str)> {
    dist::shipped_binaries()
        .iter()
        .map(|b| (b.image_path, b.dest))
        .collect()
}

/// A fresh throwaway directory (pid + per-process counter for uniqueness — no wall-clock
/// read, #619), the `fdb_image.rs` fixture pattern.
fn fixture_root(label: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "wyrd-dist-{label}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("create fixture dir");
    dir
}

/// The text test's local copy of the set IS the production table. That file must spell
/// the set itself (it is the red-earning test and names no `xtask::dist` symbol), so a
/// table change lands here first, naming the file to update.
#[test]
fn the_shipped_binary_table_is_the_text_tests_local_set() {
    assert_eq!(
        production_pairs(),
        layout::EXPECTED_BINARIES.to_vec(),
        "xtask::dist::shipped_binaries() changed — update EXPECTED_BINARIES in \
         xtask/tests/dist_two_binary_layout.rs to match (that file spells the set itself: \
         it is the red-earning text test and imports nothing from xtask::dist)"
    );
}

/// The SAME checker the text test runs over its local copy, run over the production
/// table: add a third entry to `shipped_binaries()` and change nothing else, and this
/// fails naming every pipeline file that lacks the new binary.
#[test]
fn every_pipeline_file_names_every_shipped_binary() {
    let texts = layout::PipelineTexts::read(&workspace_root());
    let disagreements = layout::pipeline_disagreements(&texts, &production_pairs());
    assert!(
        disagreements.is_empty(),
        "the distribution pipeline disagrees with xtask::dist::shipped_binaries():\n  {}",
        disagreements.join("\n  ")
    );
}

/// The table's own shape: `wyrd` first (the image's ENTRYPOINT), every destination under
/// `bin/` and named like its in-image path, and in-image paths, destinations, names and
/// host extraction paths all pairwise distinct — no two entries can extract onto one
/// file or stage onto one path.
#[test]
fn the_shipped_binary_table_is_well_formed() {
    let table = dist::shipped_binaries();
    assert_eq!(
        table.first().map(|b| b.name()),
        Some("wyrd"),
        "wyrd is the roles binary"
    );
    let root = workspace_root();
    let mut seen_image = Vec::new();
    let mut seen_dest = Vec::new();
    let mut seen_name = Vec::new();
    let mut seen_extracted = Vec::new();
    for b in &table {
        let name = b.name();
        assert!(
            !name.is_empty() && !name.contains('/'),
            "bad name for {b:?}"
        );
        assert_eq!(
            b.dest,
            format!("bin/{name}"),
            "{b:?}: tarball destination must be bin/<name>"
        );
        assert!(
            b.image_path.starts_with('/') && b.image_path.ends_with(&format!("/{name}")),
            "{b:?}: the image must carry the binary under the same file name"
        );
        let extracted = dist::extracted_binary_path(&root, b);
        assert_eq!(
            extracted,
            root.join(dist::EXTRACTED_DIR).join(name),
            "{b:?}: extraction lands in {} under its own name",
            dist::EXTRACTED_DIR
        );
        for (seen, value, what) in [
            (&mut seen_image, b.image_path.to_string(), "in-image path"),
            (&mut seen_dest, b.dest.to_string(), "tarball destination"),
            (&mut seen_name, name.to_string(), "name"),
            (
                &mut seen_extracted,
                extracted.to_string_lossy().into_owned(),
                "host extraction path",
            ),
        ] {
            assert!(
                !seen.contains(&value),
                "duplicate {what} `{value}` in the table"
            );
            seen.push(value);
        }
    }
}

/// The `--host` branch builds EVERY shipped binary in one `cargo build`, so a host-built
/// tarball has the same contents as an image-built one (it does not refuse, and it does
/// not silently build one bin).
#[test]
fn the_host_build_names_every_shipped_binary() {
    let table = dist::shipped_binaries();
    let args = dist::host_build_args(&table, "fdb,etcd");
    assert_eq!(&args[..3], ["build", "--release", "--locked"]);
    assert_eq!(&args[args.len() - 2..], ["--features", "fdb,etcd"]);
    let bins: Vec<&str> = args
        .windows(2)
        .filter(|w| w[0] == "--bin")
        .map(|w| w[1].as_str())
        .collect();
    let names: Vec<&str> = table.iter().map(|b| b.name()).collect();
    assert_eq!(
        bins, names,
        "the host argv must name exactly the table's binaries, in order"
    );
}

/// The extraction list is the table: one `docker cp <cid>:<image_path> <host path>` per
/// entry, each onto its own host file.
#[test]
fn the_extraction_argv_comes_from_the_table_onto_distinct_host_paths() {
    let root = workspace_root();
    let table = dist::shipped_binaries();
    let mut destinations = Vec::new();
    for b in &table {
        let args = dist::docker_cp_args("c0ffee", b, &root);
        assert_eq!(args.len(), 3, "{args:?}");
        assert_eq!(args[0], "cp");
        assert_eq!(args[1], format!("c0ffee:{}", b.image_path));
        assert_eq!(
            Path::new(&args[2]),
            dist::extracted_binary_path(&root, b),
            "{b:?}: docker cp must land on the pure extraction path"
        );
        assert!(
            !destinations.contains(&args[2]),
            "two entries extract onto `{}`",
            args[2]
        );
        destinations.push(args[2].clone());
    }
    assert_eq!(destinations.len(), table.len());
}

/// The real staging step over a tempdir holding two dummy binaries with DIFFERENT
/// contents: each destination lands under `bin/<name>` at 0755 and is byte-for-byte its
/// OWN source — copying one source to both destinations, or swapping them, fails.
#[test]
fn stage_binaries_copies_each_binary_to_its_own_destination() {
    use std::os::unix::fs::PermissionsExt;

    let dir = fixture_root("stage");
    let sources = dir.join("sources");
    std::fs::create_dir_all(&sources).unwrap();
    let table = dist::shipped_binaries();
    // Distinct per-binary payloads, keyed by the table so a third entry gets its own.
    let payload = |name: &str| format!("{name}-binary payload\n").into_bytes();
    for b in &table {
        std::fs::write(sources.join(b.name()), payload(b.name())).unwrap();
    }
    let stage = dir.join("stage");

    let result = dist::stage_binaries(&table, &sources, &stage);
    let staged: Vec<_> = std::fs::read_dir(stage.join("bin"))
        .map(|rd| {
            rd.map(|e| e.unwrap().file_name().into_string().unwrap())
                .collect()
        })
        .unwrap_or_default();
    let observed: Vec<(String, Vec<u8>, u32)> = table
        .iter()
        .map(|b| {
            let path = stage.join(b.dest);
            (
                b.dest.to_string(),
                std::fs::read(&path).unwrap_or_default(),
                std::fs::metadata(&path)
                    .map(|m| m.permissions().mode() & 0o777)
                    .unwrap_or(0),
            )
        })
        .collect();
    std::fs::remove_dir_all(&dir).ok();

    result.expect("staging the table over complete sources succeeds");
    for (b, (dest, bytes, mode)) in table.iter().zip(&observed) {
        assert_eq!(
            bytes,
            &payload(b.name()),
            "{dest} must be byte-identical to its own source"
        );
        assert_eq!(
            *mode, 0o755,
            "{dest} must be executable (0755), got {mode:o}"
        );
    }
    let mut expected: Vec<String> = table.iter().map(|b| b.name().to_string()).collect();
    let mut staged = staged;
    expected.sort();
    staged.sort();
    assert_eq!(
        staged, expected,
        "bin/ carries exactly the table's binaries"
    );
}

/// A source directory missing one of the table's binaries is an error naming it — a
/// `--host` build (or an extraction) that produced one binary cannot stage a thinner
/// tarball silently.
#[test]
fn stage_binaries_refuses_a_missing_binary() {
    let dir = fixture_root("stage-missing");
    let sources = dir.join("sources");
    std::fs::create_dir_all(&sources).unwrap();
    let table = dist::shipped_binaries();
    let (present, missing) = (table[0], table[1]);
    std::fs::write(sources.join(present.name()), b"only one").unwrap();

    let err = dist::stage_binaries(&table, &sources, &dir.join("stage"))
        .expect_err("a missing source must refuse");
    std::fs::remove_dir_all(&dir).ok();
    assert!(
        err.contains(missing.name()),
        "the refusal must name the missing binary `{}`, got: {err}",
        missing.name()
    );
}

/// The extraction directory is rebuilt EMPTY on every run: a binary an earlier run
/// extracted is gone before this run's `docker cp`s, so it can never stand in for one
/// this run failed to extract (and be staged into the tarball as if fresh).
#[test]
fn prepare_extraction_dir_discards_what_an_earlier_run_left() {
    let root = fixture_root("extract");
    let table = dist::shipped_binaries();
    for b in &table {
        let stale = dist::extracted_binary_path(&root, b);
        std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
        std::fs::write(&stale, b"left by an earlier run").unwrap();
    }

    let prepared = dist::prepare_extraction_dir(&root);
    let left: Vec<_> = std::fs::read_dir(root.join(dist::EXTRACTED_DIR))
        .map(|rd| rd.map(|e| e.unwrap().file_name()).collect())
        .unwrap_or_else(|e| panic!("the extraction dir must exist afterwards: {e}"));
    // A second call over the now-empty directory, and a first call on a root that has
    // none, both succeed with the same path.
    let again = dist::prepare_extraction_dir(&root);
    let fresh_root = fixture_root("extract-fresh");
    let fresh = dist::prepare_extraction_dir(&fresh_root);
    let fresh_is_dir = fresh_root.join(dist::EXTRACTED_DIR).is_dir();
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&fresh_root).ok();

    assert_eq!(prepared, Ok(root.join(dist::EXTRACTED_DIR)));
    assert!(
        left.is_empty(),
        "stale extracted binaries survived: {left:?}"
    );
    assert_eq!(again, Ok(root.join(dist::EXTRACTED_DIR)));
    assert_eq!(fresh, Ok(fresh_root.join(dist::EXTRACTED_DIR)));
    assert!(fresh_is_dir, "a first run must create the extraction dir");
}

/// A host that cannot provide the extraction directory is an error naming it — returned
/// by the step `obtain_binaries` runs BEFORE `docker create`, so there is no container
/// yet for the early return to leak.
#[test]
fn prepare_extraction_dir_refuses_a_path_it_cannot_make_a_directory() {
    let root = fixture_root("extract-blocked");
    // `target/dist` is a FILE, so `target/dist/extracted` can be neither listed nor made.
    let blocker = root.join(dist::EXTRACTED_DIR);
    let blocker = blocker.parent().unwrap();
    std::fs::create_dir_all(blocker.parent().unwrap()).unwrap();
    std::fs::write(blocker, b"not a directory").unwrap();

    let result = dist::prepare_extraction_dir(&root);
    std::fs::remove_dir_all(&root).ok();
    let err = result.expect_err("a blocked extraction dir must refuse");
    assert!(
        err.contains(dist::EXTRACTED_DIR),
        "the refusal must name the extraction dir, got: {err}"
    );
}

// ─── the layout checker is load-bearing: planted drifts are named ───────────────

/// Do `disagreements` include one about `file` that mentions `what`?
fn names(disagreements: &[String], file: &str, what: &str) -> bool {
    disagreements
        .iter()
        .any(|d| d.starts_with(file) && d.contains(what))
}

/// The diagnostic the table buys, exercised: a THIRD entry in the set, nothing else
/// changed, and the checker names every pipeline file that lacks the new binary.
#[test]
fn a_third_binary_is_named_in_every_pipeline_file_that_lacks_it() {
    let mut set = production_pairs();
    set.push(("/usr/local/bin/wyrd-third", "bin/wyrd-third"));
    let texts = layout::PipelineTexts::read(&workspace_root());
    let out = layout::pipeline_disagreements(&texts, &set);
    for file in layout::PIPELINE_FILES {
        assert!(
            names(&out, file, "wyrd-third"),
            "{file} lacks the third binary and is not named: {out:?}"
        );
    }
}

/// The checker over the REAL pipeline texts with ONE edit planted in `file`: every `from`
/// becomes `to`. The needle must still be in the real file, so a later edit of that file
/// cannot turn a case vacuous.
fn planted(file: &str, from: &str, to: &str) -> Vec<String> {
    let mut texts = layout::PipelineTexts::read(&workspace_root());
    let text = match file {
        layout::DOCKERFILE => &mut texts.dockerfile,
        layout::INSTALL_SH => &mut texts.install_sh,
        layout::README => &mut texts.readme,
        layout::RELEASE_WORKFLOW => &mut texts.release_workflow,
        other => panic!("not a pipeline file: {other}"),
    };
    assert!(
        text.contains(from),
        "planted-drift needle no longer in {file} — update the case: {from:?}"
    );
    *text = text.replace(from, to);
    layout::pipeline_disagreements(&texts, &production_pairs())
}

/// Each case is one edit to a REAL pipeline file that takes a binary out of a stage, or
/// leaves its line in place but stops it counting — the edits that would keep a release
/// green while the validator is not built, not installed, left behind by the uninstall,
/// or never really smoked. The pin names the file every time, and what was expected.
#[test]
fn the_layout_checker_names_each_planted_drift() {
    const COPY: &str =
        "COPY --from=build /src/target/release/wyrd-validate /usr/local/bin/wyrd-validate\n";
    const INSTALL: &str = "install -m 0755 \"$HERE/bin/wyrd-validate\" \"$BINDIR/wyrd-validate\"\n";
    const RM: &str = "    rm -f \"$BINDIR/wyrd-validate\"\n";
    const RUN: &str =
        "            if /usr/local/bin/wyrd-validate >/tmp/wyrd-validate-usage.txt 2>&1; then\n";
    const GREP: &str = "            grep -q 'usage: wyrd-validate' /tmp/wyrd-validate-usage.txt\n";
    const GONE: &str = "            test ! -e /usr/local/bin/wyrd-validate\n";
    const UNINSTALL: &str = "            ./install.sh --uninstall\n";
    const WYRD_GONE: &str = "            test ! -e /usr/local/bin/wyrd\n";
    const SCRIPT_END: &str = "            test -d /etc/wyrd\n          \"\n";
    const RELOAD: &str = "    if systemd_running; then systemctl daemon-reload; fi\n";
    const PURGE: &str = "    if [ \"$PURGE\" = 1 ]; then\n";
    let rm_before_purge = format!("{RM}{RELOAD}{PURGE}");
    let uninstall_then_gone = format!("{UNINSTALL}{WYRD_GONE}{GONE}");
    let (dockerfile, install, workflow) = (
        layout::DOCKERFILE,
        layout::INSTALL_SH,
        layout::RELEASE_WORKFLOW,
    );
    // (case, file, from, to, what the finding must mention)
    let cases: [(&str, &str, &str, String, &str); 18] = [
        (
            "--bin dropped from the image build",
            dockerfile,
            " --bin wyrd-validate",
            String::new(),
            "--bin wyrd-validate",
        ),
        (
            "COPY commented out",
            dockerfile,
            COPY,
            format!("# {COPY}"),
            "wyrd-validate",
        ),
        (
            "COPY lands somewhere else",
            dockerfile,
            COPY,
            COPY.replace("/usr/local/bin/", "/opt/"),
            "wyrd-validate",
        ),
        (
            "COPY folded into the instruction above it",
            dockerfile,
            COPY,
            format!("RUN true \\\n{COPY}"),
            "wyrd-validate",
        ),
        (
            "install suppressed by || true",
            install,
            INSTALL,
            INSTALL.replace('\n', " || true\n"),
            "wyrd-validate",
        ),
        (
            "install behind an existence guard",
            install,
            INSTALL,
            format!("if [ -f \"$HERE/bin/wyrd-validate\" ]; then\n{INSTALL}fi\n"),
            "wyrd-validate",
        ),
        (
            "uninstall rm deleted",
            install,
            RM,
            String::new(),
            "wyrd-validate",
        ),
        (
            "uninstall rm made to run only on --purge",
            install,
            &rm_before_purge,
            format!("{RELOAD}{PURGE}    {RM}"),
            "wyrd-validate",
        ),
        (
            "ROLES grows the validator",
            install,
            "ROLES=\"d-server custodian s3\"",
            "ROLES=\"d-server custodian s3 wyrd-validate\"".to_string(),
            "ROLES lists `wyrd-validate`",
        ),
        (
            "the ROLES line gone, so the role check has nothing to read",
            install,
            "ROLES=\"d-server custodian s3\"",
            "ROLE_LIST=\"d-server custodian s3\"".to_string(),
            "no `ROLES=` line",
        ),
        (
            "validator run commented out",
            workflow,
            RUN,
            "            # TODO smoke /usr/local/bin/wyrd-validate later\n".to_string(),
            "wyrd-validate",
        ),
        (
            "usage grep deleted",
            workflow,
            GREP,
            String::new(),
            "usage: wyrd-validate",
        ),
        (
            "usage grep suppressed by || true",
            workflow,
            GREP,
            GREP.replace('\n', " || true\n"),
            "usage: wyrd-validate",
        ),
        (
            "absence check moved before the uninstall",
            workflow,
            &uninstall_then_gone,
            format!("{GONE}{UNINSTALL}{WYRD_GONE}"),
            "install.sh --uninstall",
        ),
        (
            // A bare `"` in a script comment ends the `sh -c "…"` word early.
            "a quoted word in a script comment above the validator run",
            workflow,
            RUN,
            format!("            # would read as \"refused correctly\"\n{RUN}"),
            "wyrd-validate",
        ),
        (
            "set -n at the top of the script (nothing below it runs)",
            workflow,
            "            apt-get update -qq >/dev/null\n",
            "            set -n\n            apt-get update -qq >/dev/null\n".to_string(),
            "set -n",
        ),
        (
            "the step allowed to fail, keyed on below its script",
            workflow,
            SCRIPT_END,
            format!("{SCRIPT_END}\n        continue-on-error: true\n"),
            "continue-on-error",
        ),
        (
            "the step skipped by an if: key",
            workflow,
            "      - name: smoke the installer in a bookworm container\n",
            "      - name: smoke the installer in a bookworm container\n        if: false\n"
                .to_string(),
            "if: false",
        ),
    ];
    for (case, file, from, to, what) in cases {
        let out = planted(file, from, &to);
        assert!(
            names(&out, file, what),
            "{case}: expected a disagreement about {file} mentioning `{what}`, got {out:?}"
        );
    }

    // The README is held by name, not by exact text: fold the validator into `wyrd`.
    let out = planted(layout::README, "wyrd-validate", "wyrd");
    assert!(
        names(&out, layout::README, "bin/wyrd-validate"),
        "a README that no longer names the validator is not named: {out:?}"
    );
}
