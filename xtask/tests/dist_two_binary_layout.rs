//! The two-binary layout contract (#742): every stage of the distribution pipeline names
//! the SAME shipped-binary set. The production Dockerfile builds and copies each binary,
//! `install.sh` installs each and removes each on `--uninstall`, the release smoke step
//! runs each installed binary and asserts each is gone after the uninstall, and the
//! tarball README describes each. Declared once (`xtask::dist::shipped_binaries()`),
//! duplication checked by the gate: Docker, POSIX sh, YAML and Markdown cannot read a
//! Rust function, so what this buys is the `FDB_VERSION` shape of
//! `xtask/tests/fdb_image.rs` — one declaration, and a gate that names every file that
//! drifts from it.
//!
//! HOW a file is held to the set: by EXACT TEXT generated from the set, the way
//! `fdb_image.rs` pins its validator smoke step. The checker reads no shell, YAML or
//! Dockerfile syntax and does not try to recognise the ways a line can be silenced. It
//! builds the text the file must contain — the Dockerfile's build and copy lines,
//! `install.sh` from its `--uninstall` path through its binary installs, the release
//! workflow's whole smoke step — and compares. A `|| true`, a guard around a line, a
//! commented-out check, a `set -n`, a check moved before the uninstall: each is simply not
//! the pinned text. Nothing about the shape of those regions is forbidden (a loop over the
//! binaries is fine); a deliberate edit is made in the file and in the pinned text here.
//!
//! What a text pin cannot do: tell a right script from a wrong one (it holds the reviewed
//! text still, it does not review it), or see outside the pinned regions (the installer's
//! argument parsing, the workflow's job and triggers). Running the real artifacts is the
//! release workflow's and `fdb-image.yml`'s half.
//!
//! Container-free, and deliberately FILE-TEXT ONLY, naming no `xtask::dist` symbol: this
//! file must compile — and fail — against a tree with the production change reverted, so
//! it carries its own copy of the expected set ([`EXPECTED_BINARIES`]) and holds the
//! checker as a plain function of a binary set ([`pipeline_disagreements`]).
//! `xtask/tests/dist_templates.rs` includes this file as a module, runs the SAME checker
//! over the production table, and pins the local copy equal to it. So a third entry in
//! the table fails there naming every pipeline file that lacks it, and the equality
//! assertion says to update the local copy here — two files, one checker, one declaration.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

/// The expected set as `(in-image path, tarball destination)` — the shape of the
/// production table, spelled locally (see the module docs for why it is not imported).
/// The FIRST entry is the roles binary (`wyrd`, which links `libfdb_c`); every other entry
/// is a tool shipped beside it, which must run on a host with no FoundationDB client.
pub const EXPECTED_BINARIES: [(&str, &str); 2] = [
    ("/usr/local/bin/wyrd", "bin/wyrd"),
    ("/usr/local/bin/wyrd-validate", "bin/wyrd-validate"),
];

pub const DOCKERFILE: &str = "deploy/docker/wyrd/Dockerfile";
pub const INSTALL_SH: &str = "deploy/dist/install.sh";
pub const README: &str = "deploy/dist/README.md";
pub const RELEASE_WORKFLOW: &str = ".github/workflows/release.yml";
/// The four files the checker reads, relative to the workspace root.
pub const PIPELINE_FILES: [&str; 4] = [DOCKERFILE, INSTALL_SH, README, RELEASE_WORKFLOW];

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

/// The file name of a tarball destination (`bin/wyrd-validate` → `wyrd-validate`): the
/// cargo `--bin` name, the installed name, and the name the smoke step runs.
pub fn binary_name(dest: &str) -> &str {
    dest.rsplit('/').next().unwrap_or(dest)
}

/// The texts of [`PIPELINE_FILES`] — what the checker judges. The fields are public so a
/// test can plant one drift in a real text and run the same checker over it.
pub struct PipelineTexts {
    pub dockerfile: String,
    pub install_sh: String,
    pub readme: String,
    pub release_workflow: String,
}

impl PipelineTexts {
    /// Read the four files under `root`. A missing one panics: that is a broken checkout,
    /// not a drift to report.
    pub fn read(root: &Path) -> Self {
        let [dockerfile, install_sh, readme, release_workflow] = PIPELINE_FILES.map(|rel| {
            std::fs::read_to_string(root.join(rel))
                .unwrap_or_else(|e| panic!("read pipeline file {rel}: {e}"))
        });
        Self {
            dockerfile,
            install_sh,
            readme,
            release_workflow,
        }
    }
}

/// Every way a pipeline file disagrees with `binaries`, as `<file>: <what>` lines — empty
/// when every stage names exactly the set. A plain function of the set, so the same
/// checker runs over the local copy here and over the production table in
/// `dist_templates.rs`.
pub fn pipeline_disagreements(texts: &PipelineTexts, binaries: &[(&str, &str)]) -> Vec<String> {
    let names: Vec<&str> = binaries.iter().map(|(_, dest)| binary_name(dest)).collect();
    let Some((roles, tools)) = names.split_first() else {
        return vec!["the shipped-binary set is empty — the pipeline ships nothing".to_string()];
    };
    let mut out = dockerfile_disagreements(&texts.dockerfile, binaries);
    out.extend(install_sh_disagreements(&texts.install_sh, &names));
    // The release smoke installs with no `--prefix`, so it finds the binaries under the
    // installer's DEFAULT prefix — read off install.sh, not assumed.
    match default_prefix(&texts.install_sh) {
        Some(prefix) => out.extend(release_smoke_disagreements(
            &texts.release_workflow,
            &smoke_step(&prefix, roles, tools),
        )),
        None => out.push(format!(
            "{INSTALL_SH}: no default `PREFIX=/…` line — the release smoke step's installed \
             paths have no source of truth"
        )),
    }
    out.extend(readme_disagreements(&texts.readme, binaries));
    out
}

// ─── exact-text pins ───────────────────────────────────────────────────────────

/// What every pin finding ends with.
const BOTH_PLACES: &str = "This region is pinned as exact text generated from the \
    shipped-binary set: change the file and the pinned text in \
    xtask/tests/dist_two_binary_layout.rs together";

/// Find `pin` (whole lines, in order, nothing between them) in `lines`: `Ok` with the
/// lines that follow it, or `Err` naming the first line at which the file stops matching
/// — so a failure says what to edit, not just that something differs.
fn after_pin<'l, 't>(lines: &'l [&'t str], pin: &str) -> Result<&'l [&'t str], String> {
    let want: Vec<&str> = pin.lines().collect();
    let run = |start: usize| {
        want.iter()
            .zip(&lines[start..])
            .take_while(|(w, h)| w == h)
            .count()
    };
    // The start that matches the most pinned lines (the first such, on a tie).
    let (start, matched) = (0..lines.len())
        .map(|start| (start, run(start)))
        .fold((0, 0), |best, at| if at.1 > best.1 { at } else { best });
    if matched == want.len() {
        return Ok(&lines[start + matched..]);
    }
    if matched == 0 {
        return Err(format!(
            "the pinned text's first line `{}` is not a line of the file",
            want[0]
        ));
    }
    Err(format!(
        "line {}: expected `{}`, found `{}`",
        start + matched + 1,
        want[matched],
        lines
            .get(start + matched)
            .copied()
            .unwrap_or("<end of file>")
    ))
}

/// Is `line` a whole line of `text` — and not the tail of a `\`-continued one?
fn has_line(text: &str, line: &str) -> bool {
    let mut continued = false;
    text.lines().any(|l| {
        let found = l == line && !continued;
        continued = l.trim_end().ends_with('\\');
        found
    })
}

// ─── per-file checks ───────────────────────────────────────────────────────────

/// (1) The Dockerfile compiles exactly the set in ONE `cargo build`, and copies each
/// binary into the runtime stage at its in-image path — each an exact line.
fn dockerfile_disagreements(text: &str, binaries: &[(&str, &str)]) -> Vec<String> {
    let mut out = Vec::new();
    let bins: String = binaries
        .iter()
        .map(|(_, dest)| format!(" --bin {}", binary_name(dest)))
        .collect();
    let build =
        format!("RUN cargo build --release --locked{bins} ${{FEATURES:+--features \"$FEATURES\"}}");
    if !has_line(text, &build) {
        out.push(format!(
            "{DOCKERFILE}: no line is exactly `{build}` — the image build does not compile \
             exactly the shipped set"
        ));
    }
    for (image_path, dest) in binaries {
        let name = binary_name(dest);
        let copy = format!("COPY --from=build /src/target/release/{name} {image_path}");
        if !has_line(text, &copy) {
            out.push(format!(
                "{DOCKERFILE}: no line is exactly `{copy}` — the runtime stage does not carry \
                 `{name}` where `cargo xtask dist` extracts it from"
            ));
        }
    }
    out
}

/// `install.sh`'s `--uninstall` path, up to the per-binary removals.
const UNINSTALL_HEAD: &str = r#"if [ "$UNINSTALL" = 1 ]; then
    # Confirm the purge BEFORE any removal: asking after the units/binary are
    # gone would leave a declining operator half-uninstalled while being told
    # nothing happened.
    if [ "$PURGE" = 1 ] && [ "$YES" != 1 ]; then
        echo "--purge will DELETE $CONFDIR and $DATADIR, including any fragment data."
        printf 'Type "purge" to confirm: '
        read -r answer
        [ "$answer" = purge ] || { echo "aborted; nothing removed."; exit 1; }
    fi
    for role in $ROLES; do
        unit=wyrd-$role.service
        if systemd_running && [ -f "$UNITDIR/$unit" ]; then
            systemctl disable --now "$unit" 2>/dev/null || true
        fi
        rm -f "$UNITDIR/$unit"
    done
"#;

/// `install.sh`'s install path, from its banner up to the per-binary installs.
const INSTALL_HEAD: &str = r#"
# ── install ──────────────────────────────────────────────────────────────────

# System user the units run as. Dynamic uid on bare metal; the OCI image's fixed
# uid 10001 never shares a filesystem with this host layout, so no need to match.
if ! getent passwd wyrd >/dev/null 2>&1; then
    useradd --system --user-group --no-create-home --shell /usr/sbin/nologin wyrd
fi

install -d -m 0755 "$BINDIR"
# Every shipped binary. The validator is a binary and nothing else: no unit, no
# /etc/wyrd entry (ROLES above must not grow it — that list drives units, env files
# and `systemctl disable`).
"#;

/// `install.sh` from the top of its `--uninstall` path to the blank line after its binary
/// installs, as ONE text: the whole uninstall path (every binary removed, unconditionally,
/// and named in the summary), then the install path as far as the binary installs (each a
/// top-level statement). Whole, so that nothing inside it — a guard, a `|| true`, an
/// early `exit` — can come between a binary and its install or removal.
fn installer_pin(names: &[&str]) -> String {
    let mut pin = String::from(UNINSTALL_HEAD);
    for name in names {
        pin.push_str(&format!("    rm -f \"$BINDIR/{name}\"\n"));
    }
    let removed: Vec<String> = names.iter().map(|n| format!("$BINDIR/{n}")).collect();
    let removed = removed.join(", ");
    pin.push_str(&format!(
        r#"    if systemd_running; then systemctl daemon-reload; fi
    if [ "$PURGE" = 1 ]; then
        rm -rf "$CONFDIR" "$DATADIR"
        echo "wyrd uninstalled ({removed}, units); purged $CONFDIR and $DATADIR."
    else
        echo "wyrd uninstalled ({removed}, units). Config and data kept:"
        echo "  $CONFDIR  $DATADIR"
    fi
    exit 0
fi
"#
    ));
    pin.push_str(INSTALL_HEAD);
    for name in names {
        pin.push_str(&format!(
            "install -m 0755 \"$HERE/bin/{name}\" \"$BINDIR/{name}\"\n"
        ));
    }
    pin.push('\n');
    pin
}

/// (2) `install.sh` installs each binary from `bin/<name>`, removes each on `--uninstall`
/// ([`installer_pin`]), and lists none of them as a role.
fn install_sh_disagreements(text: &str, names: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    match text.lines().find_map(|l| l.strip_prefix("ROLES=")) {
        None => out.push(format!(
            "{INSTALL_SH}: no `ROLES=` line — cannot tell whether a shipped binary is listed \
             as a role"
        )),
        Some(roles) => {
            let roles: Vec<&str> = roles.trim_matches('"').split_whitespace().collect();
            for name in names.iter().filter(|name| roles.contains(name)) {
                out.push(format!(
                    "{INSTALL_SH}: ROLES lists `{name}` — a shipped binary is not a role (it \
                     gets no unit, no /etc/wyrd env file and no systemctl)"
                ));
            }
        }
    }
    let lines: Vec<&str> = text.lines().collect();
    if let Err(why) = after_pin(&lines, &installer_pin(names)) {
        out.push(format!(
            "{INSTALL_SH}: its --uninstall path and binary installs are not the pinned text \
             — {why}. {BOTH_PLACES}"
        ));
    }
    out
}

/// `install.sh`'s default `PREFIX=` (the first unindented absolute assignment).
fn default_prefix(install: &str) -> Option<String> {
    install
        .lines()
        .find_map(|l| l.strip_prefix("PREFIX="))
        .filter(|v| v.starts_with('/'))
        .map(|v| v.trim().to_string())
}

/// The release smoke step, down to where the tools are run.
const SMOKE_HEAD: &str = r#"      - name: smoke the installer in a bookworm container
        run: |
          set -eu
          tarball=$(ls target/dist/wyrd-*-x86_64-unknown-linux-gnu.tar.gz)
          fdb_version=$(sed -n 's/^ARG FDB_VERSION=//p' deploy/docker/wyrd/Dockerfile | head -1)
          docker run --rm -v "$PWD/target/dist:/dist:ro" debian:bookworm sh -eu -c "
            apt-get update -qq >/dev/null
            apt-get install -y -qq systemd curl >/dev/null
            tar xzf /dist/$(basename "$tarball") -C /tmp
            cd /tmp/wyrd-*
            ./install.sh
            systemd-analyze verify /etc/systemd/system/wyrd-*.service
            # A tool shipped beside wyrd (the validator) has no unit and no config, and
            # links no libfdb_c, so it is run BEFORE the client is installed below. Its
            # no-args invocation must print its usage and exit non-zero. The grep is what
            # fails the smoke if the binary cannot load at all: a loader error exits 127,
            # and the if-branch alone would take that for a correct refusal.
"#;

/// The FoundationDB client install that the roles binary needs before it can load.
const SMOKE_CLIENT: &str = r#"            # The binary loads libfdb_c at process start: install the exact pinned
            # client, then the no-args invocation must print usage and exit non-zero.
            curl -fsSLo /tmp/fdb.deb https://github.com/apple/foundationdb/releases/download/${fdb_version}/foundationdb-clients_${fdb_version}-1_amd64.deb
            dpkg -i /tmp/fdb.deb >/dev/null
"#;

/// The idempotence check and the uninstall, up to the per-binary absence checks.
const SMOKE_UNINSTALL: &str = r#"            # Idempotence: a re-run upgrades without clobbering live config.
            echo '# operator-owned' >>/etc/wyrd/d-server.env
            ./install.sh
            grep -q 'operator-owned' /etc/wyrd/d-server.env
            # Uninstall removes every shipped binary and the units, keeps config and data.
            ./install.sh --uninstall
"#;

/// The rest of the step, through the script's closing quote.
const SMOKE_TAIL: &str = r#"            test ! -e /etc/systemd/system/wyrd-d-server.service
            test -d /etc/wyrd
          "
"#;

/// The release workflow's whole smoke step for a set whose roles binary is `roles` and
/// whose other binaries are `tools`, installed under `prefix`. Per binary: a block that
/// runs it with no arguments (the `if` lets the expected non-zero exit through; the grep
/// on the next line is what fails the step when the binary cannot load at all — exit 127
/// skips the if-branch too), and a `test ! -e` after the uninstall. Each tool's block
/// sits BEFORE the FoundationDB client is installed, the roles binary's after it.
fn smoke_step(prefix: &str, roles: &str, tools: &[&str]) -> String {
    let block = |name: &str, usage: &str| {
        format!(
            r#"            if {prefix}/bin/{name} >/tmp/{name}-usage.txt 2>&1; then
              echo '{name} with no arguments must exit non-zero'; exit 1
            fi
            grep -q '{usage}' /tmp/{name}-usage.txt
"#
        )
    };
    let mut step = String::from(SMOKE_HEAD);
    for tool in tools {
        step.push_str(&block(tool, &format!("usage: {tool}")));
    }
    step.push_str(SMOKE_CLIENT);
    // `wyrd` prints a bare `usage:` line, then one line per role.
    step.push_str(&block(roles, "usage:"));
    step.push_str(SMOKE_UNINSTALL);
    for name in std::iter::once(&roles).chain(tools) {
        step.push_str(&format!("            test ! -e {prefix}/bin/{name}\n"));
    }
    step.push_str(SMOKE_TAIL);
    step
}

/// (3) The release workflow's smoke step is exactly `step` ([`smoke_step`]), and nothing
/// is keyed onto it below its script.
fn release_smoke_disagreements(workflow: &str, step: &str) -> Vec<String> {
    let lines: Vec<&str> = workflow.lines().collect();
    let rest = match after_pin(&lines, step) {
        Ok(rest) => rest,
        Err(why) => {
            return vec![format!(
                "{RELEASE_WORKFLOW}: the `smoke the installer` step is not the pinned text — \
                 {why}. {BOTH_PLACES}"
            )];
        }
    };
    // YAML lets a key follow the script after a blank line or a comment, and it would
    // still belong to this step: the next thing must be the next step.
    let next = rest.iter().find(|l| {
        let l = l.trim();
        !l.is_empty() && !l.starts_with('#')
    });
    match next {
        Some(next) if !next.starts_with("      - ") => vec![format!(
            "{RELEASE_WORKFLOW}: `{}` follows the smoke step's script — a key there \
             (`continue-on-error:`, `if:`) belongs to the step and can skip it or let it \
             fail quietly. The next thing after the script must be the next step",
            next.trim()
        )],
        _ => Vec::new(),
    }
}

/// (4) The README names each binary's tarball destination and describes each on a line of
/// its own — distinguishing what each is for rather than listing them together.
fn readme_disagreements(text: &str, binaries: &[(&str, &str)]) -> Vec<String> {
    let mut out = Vec::new();
    for (_, dest) in binaries {
        let name = binary_name(dest);
        if !has_token(text, dest) {
            out.push(format!(
                "{README}: never names `{dest}` — an operator reading the README cannot know \
                 the tarball carries `{name}`"
            ));
        }
        let own_line = text.lines().any(|l| {
            has_token(l, name)
                && binaries
                    .iter()
                    .map(|(_, other)| binary_name(other))
                    .all(|other| other == name || !has_token(l, other))
        });
        if !own_line {
            out.push(format!(
                "{README}: no line describes `{name}` on its own (every mention sits beside \
                 another shipped binary) — the README must distinguish what each binary is for"
            ));
        }
    }
    out
}

fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Does `text` contain `needle` as a whole name — not as a prefix or suffix of a longer
/// one (`bin/wyrd` is not satisfied by `bin/wyrd-validate`)?
fn has_token(text: &str, needle: &str) -> bool {
    text.match_indices(needle).any(|(start, _)| {
        let before = text[..start].chars().next_back();
        let after = text[start + needle.len()..].chars().next();
        !before.is_some_and(is_name_char) && !after.is_some_and(is_name_char)
    })
}

// ─── the contract ──────────────────────────────────────────────────────────────

/// The local expected set is itself well-formed: distinct names, each destination under
/// `bin/`, each in-image path naming the same file — so the checks above can derive one
/// name per entry.
#[test]
fn the_expected_set_is_well_formed() {
    let mut names = Vec::new();
    for (image_path, dest) in EXPECTED_BINARIES {
        let name = binary_name(dest);
        assert!(
            !name.is_empty() && name.chars().all(is_name_char),
            "bad name in `{dest}`"
        );
        assert_eq!(
            dest,
            format!("bin/{name}"),
            "tarball destination must be bin/<name>"
        );
        assert!(
            image_path.starts_with('/') && image_path.ends_with(&format!("/{name}")),
            "in-image path `{image_path}` must be absolute and name `{name}`"
        );
        assert!(!names.contains(&name), "duplicate binary name `{name}`");
        names.push(name);
    }
}

/// Every one of the four pipeline files names every expected binary — the Dockerfile
/// builds and copies it, install.sh installs and removes it, the release smoke runs it
/// and asserts its absence after uninstall, the README describes it. Red on a tree whose
/// pipeline still ships one binary.
#[test]
fn every_pipeline_file_names_every_expected_binary() {
    let texts = PipelineTexts::read(&workspace_root());
    let disagreements = pipeline_disagreements(&texts, &EXPECTED_BINARIES);
    assert!(
        disagreements.is_empty(),
        "the distribution pipeline disagrees with the shipped-binary set {EXPECTED_BINARIES:?}:\n  {}",
        disagreements.join("\n  ")
    );
}
