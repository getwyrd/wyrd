//! The ten-flag argument surface, parsed strictly.
//!
//! Hand-rolled in the shape of `wyrd`'s `ParsedArgs` (`crates/server/src/cli.rs:2533-2570`)
//! so the two binaries read as one product — same `--flag value` pairs, same "needs a value"
//! wording — but **strict** where that parser is lenient. `ParsedArgs::parse` takes the next
//! token as a flag's value verbatim (`cli.rs:2550-2553`), so `--bucket --typo` silently sets
//! the bucket to `"--typo"`, and it accepts any flag name, known or not. Here every token is
//! either resolved into [`Args`] or refused by name: an unknown flag, a flag-shaped token in a
//! value slot, an empty value, a repeated flag, a stray positional, and a missing flag are all
//! errors. None of them is ever absorbed or defaulted.

use std::collections::BTreeMap;
use std::fmt;

/// Every flag `wyrd-validate` accepts, in the order the resolved configuration echoes them.
/// All ten are required: this slice has no defaults, because a defaulted `--duration` or
/// `--run-id` is exactly the value an operator did not choose — and proposal 0017 judges a
/// run against its `--duration` (§8) and scopes every delete to its `--run-id` (§5, §15).
pub const FLAGS: [&str; 10] = [
    "endpoint",
    "region",
    "bucket",
    "scenario",
    "duration",
    "workers",
    "seed",
    "out",
    "run-id",
    "driver-placement",
];

/// The parsed argument surface: one value per flag in [`FLAGS`], each exactly as given.
///
/// Values are kept as the operator typed them, and none is empty. Typing them (a duration
/// grammar, a worker count, the scenario and placement vocabularies of proposal 0017 §8/§10)
/// belongs to the slices that consume them; this slice's contract is that none is dropped or
/// invented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub scenario: String,
    pub duration: String,
    pub workers: String,
    pub seed: String,
    pub out: String,
    pub run_id: String,
    pub driver_placement: String,
}

impl Args {
    /// `(flag, value)` for every flag, in [`FLAGS`] order.
    pub fn pairs(&self) -> [(&'static str, &str); 10] {
        [
            ("endpoint", &self.endpoint),
            ("region", &self.region),
            ("bucket", &self.bucket),
            ("scenario", &self.scenario),
            ("duration", &self.duration),
            ("workers", &self.workers),
            ("seed", &self.seed),
            ("out", &self.out),
            ("run-id", &self.run_id),
            ("driver-placement", &self.driver_placement),
        ]
    }
}

/// Why an argument list was refused. Every variant names the offending token or flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgError {
    /// A token that is not `--`-prefixed where a flag was expected. `wyrd-validate` takes no
    /// positional arguments.
    Unexpected(String),
    /// A `--`-prefixed token that is not one of [`FLAGS`].
    UnknownFlag(String),
    /// A flag was the last token, with nothing after it.
    NeedsValue(&'static str),
    /// A flag's value slot held a `--`-prefixed token — the likely mistake is a forgotten
    /// value (`--bucket --typo`), so neither token is guessed at.
    FlagInValueSlot { flag: &'static str, token: String },
    /// A flag given an empty value — typically `--run-id "$RUN_ID"` with the variable unset.
    /// That is a missing value, not a chosen one.
    EmptyValue(&'static str),
    /// A flag given twice. Keeping either value would silently discard the other.
    Repeated(&'static str),
    /// Required flags that were never given, in [`FLAGS`] order.
    Missing(Vec<&'static str>),
}

impl fmt::Display for ArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unexpected(token) => write!(
                f,
                "unexpected argument `{token}`: wyrd-validate takes only `--flag value` pairs"
            ),
            Self::UnknownFlag(token) => write!(f, "unrecognised flag `{token}`"),
            Self::NeedsValue(flag) => write!(f, "flag `--{flag}` needs a value"),
            Self::FlagInValueSlot { flag, token } => write!(
                f,
                "flag `--{flag}` needs a value, but the next argument `{token}` is itself a \
                 flag; refusing to take `{token}` as the value of `--{flag}`"
            ),
            Self::EmptyValue(flag) => write!(f, "flag `--{flag}` was given an empty value"),
            Self::Repeated(flag) => write!(
                f,
                "flag `--{flag}` given more than once; refusing to pick one of its values"
            ),
            Self::Missing(flags) => {
                let names: Vec<String> = flags.iter().map(|flag| format!("`--{flag}`")).collect();
                write!(f, "missing required flag(s): {}", names.join(", "))
            }
        }
    }
}

impl std::error::Error for ArgError {}

/// Parse the arguments after the program name. Strict: see the module docs.
pub fn parse(args: &[String]) -> Result<Args, ArgError> {
    let mut given: BTreeMap<&'static str, String> = BTreeMap::new();
    let mut i = 0;
    while i < args.len() {
        let token = &args[i];
        let name = token
            .strip_prefix("--")
            .ok_or_else(|| ArgError::Unexpected(token.clone()))?;
        let flag = FLAGS
            .iter()
            .copied()
            .find(|known| *known == name)
            .ok_or_else(|| ArgError::UnknownFlag(token.clone()))?;
        let value = args.get(i + 1).ok_or(ArgError::NeedsValue(flag))?;
        if value.starts_with("--") {
            return Err(ArgError::FlagInValueSlot {
                flag,
                token: value.clone(),
            });
        }
        if value.is_empty() {
            return Err(ArgError::EmptyValue(flag));
        }
        if given.insert(flag, value.clone()).is_some() {
            return Err(ArgError::Repeated(flag));
        }
        i += 2;
    }

    // A flag that was never given is recorded as missing, and the placeholder it leaves in
    // the struct is discarded with the whole `Args` below — it can never reach a caller.
    let mut missing: Vec<&'static str> = Vec::new();
    let mut take = |flag: &'static str| {
        given.remove(flag).unwrap_or_else(|| {
            missing.push(flag);
            String::new()
        })
    };
    let parsed = Args {
        endpoint: take("endpoint"),
        region: take("region"),
        bucket: take("bucket"),
        scenario: take("scenario"),
        duration: take("duration"),
        workers: take("workers"),
        seed: take("seed"),
        out: take("out"),
        run_id: take("run-id"),
        driver_placement: take("driver-placement"),
    };
    if !missing.is_empty() {
        return Err(ArgError::Missing(missing));
    }
    Ok(parsed)
}

/// The one-line usage summary appended to every argument error.
pub fn usage() -> String {
    let flags: Vec<String> = FLAGS
        .iter()
        .map(|flag| format!("--{flag} <{}>", flag.to_uppercase().replace('-', "_")))
        .collect();
    format!("usage: wyrd-validate {}", flags.join(" "))
}
