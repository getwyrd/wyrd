//! The `wyrd-validate` argument surface (proposal 0017; the skeleton slice).
//!
//! Binds the invariant *an operator-facing binary never silently discards an argument it was
//! given*: every one of the ten flags is echoed with its value or refused by name, credentials
//! resolve AWS → `WYRD_S3_*` → refuse without ever echoing the secret, and a token the parser
//! does not understand — an unknown flag, or a flag-shaped token in a value slot — is refused.
//!
//! Binary-level tests run the real `wyrd-validate` with a cleared child environment
//! (`env_clear`) holding only the variables each test names, plus `LLVM_PROFILE_FILE` when a
//! coverage run set it (see `bin`); the test process's own env is never mutated. The
//! credential matrix runs over the library's injected lookup for the same reason.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{self, Write};
use std::process::{Command, Output};

use wyrd_validate::{resolve_config, run, CredentialSource, EXIT_IO, EXIT_OK, EXIT_USAGE, FLAGS};

/// A distinct value for every flag, so a swapped or dropped value cannot pass.
const GIVEN: [(&str, &str); 10] = [
    ("endpoint", "http://127.0.0.1:9000"),
    ("region", "eu-central-1"),
    ("bucket", "validate-bucket"),
    ("scenario", "smoke"),
    ("duration", "2m"),
    ("workers", "7"),
    ("seed", "424242"),
    ("out", "/var/lib/wyrd-validate/out"),
    ("run-id", "run-0017"),
    ("driver-placement", "external"),
];

const WYRD_ID: &str = "WYRDKEYID0001";
const WYRD_SECRET: &str = "wyrd-secret-must-never-print";
const AWS_ID: &str = "AKIAAWSKEYID0001";
const AWS_SECRET: &str = "aws-secret-must-never-print";

/// An environment: variable name → raw value (raw so a non-UTF-8 value can be expressed).
type Env = Vec<(&'static str, OsString)>;

fn env(pairs: &[(&'static str, &str)]) -> Env {
    pairs
        .iter()
        .map(|(name, value)| (*name, OsString::from(value)))
        .collect()
}

fn wyrd_pair() -> Env {
    env(&[
        ("WYRD_S3_ACCESS_KEY", WYRD_ID),
        ("WYRD_S3_SECRET_KEY", WYRD_SECRET),
    ])
}

fn aws_pair() -> Env {
    env(&[
        ("AWS_ACCESS_KEY_ID", AWS_ID),
        ("AWS_SECRET_ACCESS_KEY", AWS_SECRET),
    ])
}

fn argv(pairs: &[(&str, &str)]) -> Vec<String> {
    pairs
        .iter()
        .flat_map(|(flag, value)| [format!("--{flag}"), (*value).to_string()])
        .collect()
}

/// `GIVEN` with `flag`'s pair removed.
fn given_without(flag: &str) -> Vec<(&'static str, &'static str)> {
    GIVEN.iter().copied().filter(|(f, _)| *f != flag).collect()
}

/// Run the real binary with exactly `vars` as its environment.
fn bin(args: &[String], vars: &Env) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_wyrd-validate"));
    command.args(args).env_clear().envs(vars.iter().cloned());
    // A coverage run (`cargo llvm-cov`) tells the instrumented child where to write its
    // profile through this one variable. Clearing it would drop the child's coverage and
    // leave `default_*.profraw` files in the crate directory. It holds no credential.
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    command.output().expect("spawn wyrd-validate")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("utf-8 output")
}

/// An injected environment lookup over `vars`.
fn lookup_of(vars: &Env) -> impl Fn(&str) -> Option<OsString> {
    let map: HashMap<&'static str, OsString> = vars.iter().cloned().collect();
    move |name: &str| map.get(name).cloned()
}

/// Run the library entry point over an injected environment. Returns (status, stdout, stderr).
fn lib_run(args: &[String], vars: &Env) -> (u8, String, String) {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let status = run(args, &lookup_of(vars), &mut out, &mut err);
    (status, text(&out), text(&err))
}

// ---- Criterion 2: the ten-flag surface, bound flag by flag ----------------------------------

const USAGE: &str = "usage: wyrd-validate --endpoint <ENDPOINT> --region <REGION> \
    --bucket <BUCKET> --scenario <SCENARIO> --duration <DURATION> --workers <WORKERS> \
    --seed <SEED> --out <OUT> --run-id <RUN_ID> --driver-placement <DRIVER_PLACEMENT>";

#[test]
fn the_test_table_covers_exactly_the_declared_flags() {
    let given: Vec<&str> = GIVEN.iter().map(|(flag, _)| *flag).collect();
    assert_eq!(given, FLAGS.to_vec());
}

#[test]
fn all_ten_flags_are_echoed_each_with_its_own_value() {
    let out = bin(&argv(&GIVEN), &wyrd_pair());
    let stdout = text(&out.stdout);
    assert!(
        out.status.success(),
        "exit {:?}, stderr: {}",
        out.status,
        text(&out.stderr)
    );
    let lines: Vec<&str> = stdout.lines().collect();
    for (flag, value) in GIVEN {
        let expected = format!("  --{flag} = {value}");
        assert!(
            lines.contains(&expected.as_str()),
            "flag --{flag} not echoed with its value `{value}`; stdout:\n{stdout}"
        );
    }
}

#[test]
fn flag_order_on_the_command_line_does_not_matter() {
    let mut reversed = GIVEN;
    reversed.reverse();
    let out = bin(&argv(&reversed), &wyrd_pair());
    assert!(out.status.success(), "stderr: {}", text(&out.stderr));
    let stdout = text(&out.stdout);
    for (flag, value) in GIVEN {
        assert!(
            stdout.contains(&format!("  --{flag} = {value}\n")),
            "{stdout}"
        );
    }
}

#[test]
fn each_missing_flag_is_refused_by_name() {
    for (missing, _) in GIVEN {
        let out = bin(&argv(&given_without(missing)), &wyrd_pair());
        let stderr = text(&out.stderr);
        assert!(!out.status.success(), "missing --{missing} was accepted");
        assert!(
            stderr.contains(&format!("missing required flag(s): `--{missing}`")),
            "missing --{missing} not named; stderr: {stderr}"
        );
        assert!(
            out.stdout.is_empty(),
            "a refused run echoed a configuration"
        );
    }
}

#[test]
fn no_arguments_names_every_flag_and_prints_the_usage_line() {
    let out = bin(&[], &wyrd_pair());
    let stderr = text(&out.stderr);
    assert!(!out.status.success());
    for flag in FLAGS {
        assert!(stderr.contains(&format!("`--{flag}`")), "{stderr}");
    }
    assert!(
        stderr.lines().any(|line| line == USAGE),
        "usage line missing; stderr: {stderr}"
    );
}

#[test]
fn each_empty_value_is_refused_by_name() {
    // `--run-id "$RUN_ID"` with the variable unset: a missing value, not a chosen one.
    for (flag, _) in GIVEN {
        let mut args = vec![format!("--{flag}"), String::new()];
        args.extend(argv(&given_without(flag)));
        let (status, stdout, stderr) = lib_run(&args, &wyrd_pair());
        assert_eq!(status, EXIT_USAGE, "--{flag} \"\" was accepted: {stdout}");
        assert!(stdout.is_empty(), "{stdout}");
        assert!(
            stderr.contains(&format!("flag `--{flag}` was given an empty value")),
            "{stderr}"
        );
    }
}

// ---- Criterion 3: credential resolution, all four directions --------------------------------

enum Expect {
    /// Resolves to this pair; `source_line` is the exact echoed source line.
    Resolves {
        id: &'static str,
        secret: &'static str,
        source: CredentialSource,
        source_line: &'static str,
    },
    /// Refused; stderr must contain this phrase.
    Refused(&'static str),
}

const AWS_LINE: &str = "  credential-source = AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY";
const WYRD_LINE: &str = "  credential-source = WYRD_S3_ACCESS_KEY + WYRD_S3_SECRET_KEY";

const AWS: Expect = Expect::Resolves {
    id: AWS_ID,
    secret: AWS_SECRET,
    source: CredentialSource::Aws,
    source_line: AWS_LINE,
};
const WYRD: Expect = Expect::Resolves {
    id: WYRD_ID,
    secret: WYRD_SECRET,
    source: CredentialSource::Wyrd,
    source_line: WYRD_LINE,
};

/// One credential case, checked through both the echo (`run`) and the resolved value
/// (`resolve_config`): the right id is echoed, the right **secret** is the one resolved, and
/// no secret appears on stdout, stderr, or in `Debug` output.
fn check_credentials(case: &str, vars: &Env, expect: &Expect) {
    let args = argv(&GIVEN);
    let (status, stdout, stderr) = lib_run(&args, vars);
    for secret in [AWS_SECRET, WYRD_SECRET] {
        assert!(
            !stdout.contains(secret),
            "{case}: secret on stdout:\n{stdout}"
        );
        assert!(
            !stderr.contains(secret),
            "{case}: secret on stderr:\n{stderr}"
        );
    }
    let resolved = resolve_config(&args, &lookup_of(vars));
    match *expect {
        Expect::Resolves {
            id,
            secret,
            source,
            source_line,
        } => {
            assert_eq!(status, EXIT_OK, "{case}: {stderr}");
            let lines: Vec<&str> = stdout.lines().collect();
            let id_line = format!("  access-key-id = {id}");
            assert!(lines.contains(&id_line.as_str()), "{case}: {stdout}");
            assert!(lines.contains(&source_line), "{case}: {stdout}");
            let config = resolved.unwrap_or_else(|e| panic!("{case}: refused: {e}"));
            assert_eq!(config.credentials.access_key_id(), id, "{case}");
            assert_eq!(config.credentials.secret_access_key(), secret, "{case}");
            assert_eq!(config.credentials.source(), source, "{case}");
            let debug = format!("{config:?}");
            assert!(debug.contains(id), "{case}: {debug}");
            assert!(!debug.contains(secret), "{case}: secret in Debug: {debug}");
        }
        Expect::Refused(phrase) => {
            assert_eq!(status, EXIT_USAGE, "{case}: accepted: {stdout}");
            assert!(stdout.is_empty(), "{case}: {stdout}");
            assert!(stderr.contains(phrase), "{case}: {stderr}");
            assert!(resolved.is_err(), "{case}");
        }
    }
}

fn joined(parts: &[Env]) -> Env {
    parts.concat()
}

#[test]
fn credentials_resolve_aws_then_wyrd_then_refuse() {
    let cases: [(&str, Env, Expect); 4] = [
        ("AWS pair alone", aws_pair(), AWS),
        ("Wyrd pair alone", wyrd_pair(), WYRD),
        (
            "both pairs: AWS wins",
            joined(&[wyrd_pair(), aws_pair()]),
            AWS,
        ),
        (
            "neither pair",
            Vec::new(),
            Expect::Refused(
                "no S3 credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or \
                 WYRD_S3_ACCESS_KEY and WYRD_S3_SECRET_KEY",
            ),
        ),
    ];
    for (case, vars, expect) in &cases {
        check_credentials(case, vars, expect);
    }
}

#[test]
fn half_a_pair_is_refused_in_either_direction_rather_than_skipped() {
    // Each half-pair sits in front of a complete Wyrd pair where it can: it must not quietly
    // fall through to the Wyrd identity.
    let cases: [(&str, Env, Expect); 4] = [
        (
            "AWS id without secret",
            joined(&[env(&[("AWS_ACCESS_KEY_ID", AWS_ID)]), wyrd_pair()]),
            Expect::Refused("AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is not"),
        ),
        (
            "AWS secret without id",
            joined(&[env(&[("AWS_SECRET_ACCESS_KEY", AWS_SECRET)]), wyrd_pair()]),
            Expect::Refused("AWS_SECRET_ACCESS_KEY is set but AWS_ACCESS_KEY_ID is not"),
        ),
        (
            "Wyrd id without secret",
            env(&[("WYRD_S3_ACCESS_KEY", WYRD_ID)]),
            Expect::Refused("WYRD_S3_ACCESS_KEY is set but WYRD_S3_SECRET_KEY is not"),
        ),
        (
            "Wyrd secret without id",
            env(&[("WYRD_S3_SECRET_KEY", WYRD_SECRET)]),
            Expect::Refused("WYRD_S3_SECRET_KEY is set but WYRD_S3_ACCESS_KEY is not"),
        ),
    ];
    for (case, vars, expect) in &cases {
        check_credentials(case, vars, expect);
    }
}

#[test]
fn an_empty_variable_counts_as_unset() {
    // `AWS_ACCESS_KEY_ID=` (exported empty) must not shadow a complete Wyrd pair.
    let cases: [(&str, Env, Expect); 2] = [
        (
            "empty AWS id",
            joined(&[env(&[("AWS_ACCESS_KEY_ID", "")]), wyrd_pair()]),
            WYRD,
        ),
        (
            "empty AWS pair",
            joined(&[
                env(&[("AWS_ACCESS_KEY_ID", ""), ("AWS_SECRET_ACCESS_KEY", "")]),
                wyrd_pair(),
            ]),
            WYRD,
        ),
    ];
    for (case, vars, expect) in &cases {
        check_credentials(case, vars, expect);
    }
}

/// A value that is set but is not UTF-8.
#[cfg(unix)]
fn not_utf8() -> OsString {
    use std::os::unix::ffi::OsStringExt;
    OsString::from_vec(b"AKIA\xff".to_vec())
}

#[cfg(unix)]
#[test]
fn a_non_utf8_variable_is_refused_by_name_rather_than_treated_as_unset() {
    let cases: [(&str, Env, Expect); 2] = [
        (
            "non-UTF-8 AWS id",
            joined(&[
                vec![("AWS_ACCESS_KEY_ID", not_utf8())],
                env(&[("AWS_SECRET_ACCESS_KEY", AWS_SECRET)]),
                wyrd_pair(),
            ]),
            Expect::Refused("AWS_ACCESS_KEY_ID is set but its value is not valid UTF-8"),
        ),
        (
            "non-UTF-8 AWS secret",
            joined(&[
                env(&[("AWS_ACCESS_KEY_ID", AWS_ID)]),
                vec![("AWS_SECRET_ACCESS_KEY", not_utf8())],
                wyrd_pair(),
            ]),
            Expect::Refused("AWS_SECRET_ACCESS_KEY is set but its value is not valid UTF-8"),
        ),
    ];
    for (case, vars, expect) in &cases {
        check_credentials(case, vars, expect);
    }
}

#[test]
fn the_real_binary_reads_its_environment_aws_first() {
    let out = bin(&argv(&GIVEN), &joined(&[wyrd_pair(), aws_pair()]));
    let (stdout, stderr) = (text(&out.stdout), text(&out.stderr));
    assert!(out.status.success(), "{stderr}");
    assert!(stdout.contains(&format!("  access-key-id = {AWS_ID}\n")));
    for secret in [AWS_SECRET, WYRD_SECRET] {
        assert!(!stdout.contains(secret) && !stderr.contains(secret));
    }

    let out = bin(&argv(&GIVEN), &Vec::new());
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("WYRD_S3_ACCESS_KEY"));
}

#[cfg(unix)]
#[test]
fn the_real_binary_refuses_a_non_utf8_aws_id_instead_of_signing_as_wyrd() {
    let vars = joined(&[
        vec![("AWS_ACCESS_KEY_ID", not_utf8())],
        env(&[("AWS_SECRET_ACCESS_KEY", AWS_SECRET)]),
        wyrd_pair(),
    ]);
    let out = bin(&argv(&GIVEN), &vars);
    let (stdout, stderr) = (text(&out.stdout), text(&out.stderr));
    assert!(!out.status.success(), "accepted: {stdout}");
    assert!(!stdout.contains(WYRD_ID), "{stdout}");
    assert!(
        stderr.contains("AWS_ACCESS_KEY_ID is set but its value is not valid UTF-8"),
        "{stderr}"
    );
}

// ---- Criterion 4: strict rejection, both halves ---------------------------------------------

#[test]
fn an_unrecognised_flag_is_refused_by_name() {
    let mut args = argv(&GIVEN);
    args.extend(["--end-point".to_string(), "http://elsewhere".to_string()]);
    let out = bin(&args, &wyrd_pair());
    let stderr = text(&out.stderr);
    assert!(!out.status.success(), "unknown flag accepted");
    assert!(
        stderr.contains("unrecognised flag `--end-point`"),
        "{stderr}"
    );
    assert!(out.stdout.is_empty());
}

#[test]
fn a_flag_in_any_value_slot_is_refused_naming_both_flags() {
    for (flag, _) in GIVEN {
        // `--<flag> --typo`, then every other flag with a good value, so only the value slot
        // is wrong; a lenient parser would set `<flag>` to "--typo" and exit 0.
        let mut args = vec![format!("--{flag}"), "--typo".to_string()];
        args.extend(argv(&given_without(flag)));
        let out = bin(&args, &wyrd_pair());
        let stderr = text(&out.stderr);
        assert!(!out.status.success(), "--{flag} --typo was accepted");
        assert!(stderr.contains(&format!("`--{flag}`")), "{stderr}");
        assert!(stderr.contains("`--typo`"), "{stderr}");
        assert!(out.stdout.is_empty());
    }
}

#[test]
fn a_known_flag_in_a_value_slot_is_refused_too() {
    // `--bucket --duration 2m` — the forgotten bucket must not become "--duration".
    let rest = given_without("bucket");
    let mut args = vec!["--bucket".to_string()];
    args.extend(argv(&rest));
    let out = bin(&args, &wyrd_pair());
    let stderr = text(&out.stderr);
    assert!(!out.status.success(), "stdout: {}", text(&out.stdout));
    assert!(stderr.contains("`--bucket`"), "{stderr}");
    assert!(stderr.contains(&format!("`--{}`", rest[0].0)), "{stderr}");
}

#[test]
fn a_trailing_flag_with_no_value_is_refused() {
    let mut args = argv(&GIVEN[..9]);
    args.push("--driver-placement".to_string());
    let out = bin(&args, &wyrd_pair());
    let stderr = text(&out.stderr);
    assert!(!out.status.success());
    assert!(
        stderr.contains("flag `--driver-placement` needs a value"),
        "{stderr}"
    );
}

#[test]
fn a_repeated_flag_is_refused_rather_than_last_wins() {
    let mut args = argv(&GIVEN);
    args.extend(["--duration".to_string(), "7d".to_string()]);
    let out = bin(&args, &wyrd_pair());
    let stderr = text(&out.stderr);
    assert!(!out.status.success(), "stdout: {}", text(&out.stdout));
    assert!(
        stderr.contains("flag `--duration` given more than once"),
        "{stderr}"
    );
}

#[test]
fn a_stray_positional_is_refused() {
    let mut args = argv(&GIVEN);
    args.push("smoke".to_string());
    let out = bin(&args, &wyrd_pair());
    let stderr = text(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("unexpected argument `smoke`"), "{stderr}");
}

// ---- Exit-status honesty: an echo that did not reach stdout is not a success ----------------

/// A stdout that fails on write, on flush, or neither.
#[derive(Default)]
struct Stdout {
    fail_write: bool,
    fail_flush: bool,
    written: Vec<u8>,
    flushed: bool,
}

impl Write for Stdout {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.fail_write {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "stdout closed"));
        }
        self.written.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            return Err(io::Error::other("flush refused"));
        }
        self.flushed = true;
        Ok(())
    }
}

#[test]
fn a_failed_write_or_flush_of_the_echo_exits_non_zero() {
    let args = argv(&GIVEN);
    let lookup = lookup_of(&wyrd_pair());
    // Write fails (flush would succeed); then write succeeds and only the flush fails.
    for (fail_write, fail_flush, cause) in [
        (true, false, "stdout closed"),
        (false, true, "flush refused"),
    ] {
        let mut out = Stdout {
            fail_write,
            fail_flush,
            ..Stdout::default()
        };
        let mut err = Vec::new();
        let status = run(&args, &lookup, &mut out, &mut err);
        let stderr = text(&err);
        assert_eq!(
            status, EXIT_IO,
            "write fails: {fail_write}, flush fails: {fail_flush}"
        );
        assert!(
            stderr.contains("cannot write the resolved configuration to stdout")
                && stderr.contains(cause),
            "{stderr}"
        );
        assert!(!stderr.contains("configuration resolved"), "{stderr}");
    }

    // And the success path both writes and flushes the echo.
    let mut out = Stdout::default();
    let status = run(&args, &lookup, &mut out, &mut Vec::new());
    assert_eq!(status, EXIT_OK);
    assert!(out.flushed, "the echo was never flushed");
    assert!(text(&out.written).contains("  --run-id = run-0017\n"));
}
