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
