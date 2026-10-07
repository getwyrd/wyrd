#![forbid(unsafe_code)]
//! `wyrd-validate` — the blackbox validator (proposal 0017,
//! `docs/design/proposals/draft/0017-blackbox-validation-tool.md`).
//!
//! An out-of-process tool that drives a Wyrd deployment through its S3 front door the way a
//! client does, knowing nothing about Wyrd but its address. The binary parses the argument
//! surface, resolves credentials, echoes the resolved configuration, and exits; the library
//! also carries the S3 client every scenario calls through ([`s3`]). No scenario drives that
//! client yet: the capability matrix and `smoke` arrive in #743.
//!
//! Layering (proposal 0017 §2): everything decision-shaped lives here, pure and tested; the
//! binary (`src/main.rs`) owns only the I/O — the real argv, the real environment, the real
//! stdout/stderr — and hands them to [`run`].
//!
//! The invariant the argument surface holds: **the binary never silently discards an
//! argument it was given.** Every declared flag is either resolved into the echoed
//! configuration or refused by name, and every token the parser does not understand is
//! refused rather than absorbed. The S3 client's invariants are stated in [`s3`].

pub mod access_keys;
pub mod args;
pub mod s3;

use std::ffi::OsString;
use std::io::Write;

pub use access_keys::{resolve, CredentialError, CredentialSource, Credentials};
pub use args::{parse, usage, ArgError, Args, FLAGS};
pub use s3::{
    BodyError, Deadlines, ErrorCode, ObjectBody, Phase, PutOutcome, PutSource, S3Client, S3Error,
};

/// Exit status for a run whose validation passed. No run returns it yet: no scenario drives the
/// S3 client, so nothing has been validated to pass (see [`EXIT_INCONCLUSIVE`]).
pub const EXIT_OK: u8 = 0;
/// Exit status when writing the resolved configuration to stdout failed.
pub const EXIT_IO: u8 = 1;
/// Exit status for a refused invocation: bad arguments or no usable credentials.
pub const EXIT_USAGE: u8 = 2;
/// Exit status for a run that proved nothing: proposal 0017's vacuity rule makes such a run
/// INCONCLUSIVE — non-zero, never a pass — so a CI job or script reading only the status never
/// takes it as a green validation. This slice resolves the configuration and stops there, so
/// every run that gets that far ends here until the S3 client and scenarios are wired.
pub const EXIT_INCONCLUSIVE: u8 = 3;

/// Everything a run needs, resolved: the ten flags plus the credential pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConfig {
    pub args: Args,
    pub credentials: Credentials,
}

impl ResolvedConfig {
    /// The resolved-configuration block: one `--flag = value` line per flag, then the
    /// access-key **id** and where it came from. The secret is never part of it.
    pub fn render(&self) -> String {
        let mut block = String::from("wyrd-validate: resolved configuration\n");
        for (flag, value) in self.args.pairs() {
            block.push_str(&format!("  --{flag} = {value}\n"));
        }
        block.push_str(&format!(
            "  access-key-id = {}\n",
            self.credentials.access_key_id()
        ));
        block.push_str(&format!(
            "  credential-source = {}\n",
            self.credentials.source()
        ));
        block
    }
}

/// Parse `args` (argv without the program name) and resolve credentials over `lookup`.
/// Argument errors are reported before credential errors.
pub fn resolve_config(
    args: &[String],
    lookup: &dyn Fn(&str) -> Option<OsString>,
) -> Result<ResolvedConfig, RunError> {
    let args = parse(args).map_err(RunError::Args)?;
    let credentials = resolve(lookup).map_err(RunError::Credentials)?;
    Ok(ResolvedConfig { args, credentials })
}

/// Why a run was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError {
    Args(ArgError),
    Credentials(CredentialError),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Args(e) => write!(f, "{e}\n{}", usage()),
            Self::Credentials(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RunError {}

/// The whole program over injected I/O: resolve, echo the resolved configuration to `out`,
/// and return the exit status. A refusal goes to `err`, prefixed `wyrd-validate:`.
pub fn run(
    args: &[String],
    lookup: &dyn Fn(&str) -> Option<OsString>,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> u8 {
    match resolve_config(args, lookup) {
        Ok(config) => {
            // The echo is this slice's whole output, so a write or flush that fails is a
            // failed run, not a success with nothing printed.
            let echoed = out
                .write_all(config.render().as_bytes())
                .and_then(|()| out.flush());
            if let Err(e) = echoed {
                let _ = writeln!(
                    err,
                    "wyrd-validate: cannot write the resolved configuration to stdout: {e}"
                );
                return EXIT_IO;
            }
            // Nothing was validated, so the run is INCONCLUSIVE (proposal 0017's vacuity rule):
            // the status says so for a script, and stderr says why for an operator.
            // Best-effort: stderr is the last place left to report to.
            let _ = writeln!(
                err,
                "wyrd-validate: configuration resolved; no requests were issued and nothing \
                 was validated (no scenario drives the S3 client yet) — INCONCLUSIVE"
            );
            EXIT_INCONCLUSIVE
        }
        Err(e) => {
            let _ = writeln!(err, "wyrd-validate: {e}");
            EXIT_USAGE
        }
    }
}
