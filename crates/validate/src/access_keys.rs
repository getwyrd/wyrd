//! S3 credential resolution: the AWS pair, then the Wyrd pair, then refuse.
//!
//! Order: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` first — the names every S3 client an
//! operator already has reads — then `WYRD_S3_ACCESS_KEY`/`WYRD_S3_SECRET_KEY`, the pair
//! `wyrd s3` itself reads (`crates/server/src/cli.rs:2166`, `:2173`). Neither → refuse, the
//! same posture as `wyrd s3`: "there is no anonymous access" (`cli.rs:2168`).
//!
//! The environment is read through an injected lookup, never `std::env` directly, so the
//! whole decision is testable without mutating process env (shared across parallel test
//! threads) — the "pure decisions, injected I/O" convention `xtask` follows
//! (`xtask/src/main.rs:1559`). The lookup hands back the raw [`OsString`] (the binary passes
//! `std::env::var_os`), so "set but not UTF-8" stays distinguishable from "unset" and is
//! refused by name rather than skipped.

use std::ffi::OsString;
use std::fmt;

/// Where the resolved credentials came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    /// `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`.
    Aws,
    /// `WYRD_S3_ACCESS_KEY` + `WYRD_S3_SECRET_KEY`.
    Wyrd,
}

impl CredentialSource {
    /// Resolution order: the first complete pair wins.
    pub const ORDER: [Self; 2] = [Self::Aws, Self::Wyrd];

    /// The environment variable holding the access-key id.
    pub fn id_var(self) -> &'static str {
        match self {
            Self::Aws => "AWS_ACCESS_KEY_ID",
            Self::Wyrd => "WYRD_S3_ACCESS_KEY",
        }
    }

    /// The environment variable holding the secret key.
    pub fn secret_var(self) -> &'static str {
        match self {
            Self::Aws => "AWS_SECRET_ACCESS_KEY",
            Self::Wyrd => "WYRD_S3_SECRET_KEY",
        }
    }
}

impl fmt::Display for CredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} + {}", self.id_var(), self.secret_var())
    }
}

/// A resolved credential pair. The secret is reachable only through
/// [`Credentials::secret_access_key`]; `Debug` redacts it, so no formatting path prints it.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    source: CredentialSource,
}

impl Credentials {
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// The secret key, for signing requests. Never echo it.
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    pub fn source(&self) -> CredentialSource {
        self.source
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("source", &self.source)
            .finish()
    }
}

/// Why no credentials were resolved. Every variant names the variables to set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// Neither pair is set.
    NoneSet,
    /// One half of a pair is set and the other is not. Refused rather than falling through
    /// to the next pair: an operator who set `AWS_ACCESS_KEY_ID` meant to use it, and
    /// silently signing with a different identity would hide the mistake.
    HalfPair {
        present: &'static str,
        missing: &'static str,
    },
    /// A variable is set but its value is not valid UTF-8. Refused for the same reason as
    /// [`CredentialError::HalfPair`]: treating it as unset would fall through to the next
    /// pair and sign as a different identity.
    NotUnicode(&'static str),
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoneSet => {
                let [aws, wyrd] = CredentialSource::ORDER;
                write!(
                    f,
                    "no S3 credentials: set {} and {}, or {} and {}; there is no anonymous \
                     access",
                    aws.id_var(),
                    aws.secret_var(),
                    wyrd.id_var(),
                    wyrd.secret_var()
                )
            }
            Self::HalfPair { present, missing } => write!(
                f,
                "{present} is set but {missing} is not; set both, or neither"
            ),
            Self::NotUnicode(var) => write!(
                f,
                "{var} is set but its value is not valid UTF-8; refusing rather than treating \
                 it as unset"
            ),
        }
    }
}

impl std::error::Error for CredentialError {}

/// Resolve credentials over `lookup` (an environment-variable reader returning the raw
/// value). An empty value counts as unset, so `AWS_ACCESS_KEY_ID=` does not shadow a complete
/// Wyrd pair; a value that is not UTF-8 is refused by name.
pub fn resolve(lookup: &dyn Fn(&str) -> Option<OsString>) -> Result<Credentials, CredentialError> {
    for source in CredentialSource::ORDER {
        let id = read(lookup, source.id_var())?;
        let secret = read(lookup, source.secret_var())?;
        match (id, secret) {
            (Some(access_key_id), Some(secret_access_key)) => {
                return Ok(Credentials {
                    access_key_id,
                    secret_access_key,
                    source,
                })
            }
            (None, None) => continue,
            (Some(_), None) => {
                return Err(CredentialError::HalfPair {
                    present: source.id_var(),
                    missing: source.secret_var(),
                })
            }
            (None, Some(_)) => {
                return Err(CredentialError::HalfPair {
                    present: source.secret_var(),
                    missing: source.id_var(),
                })
            }
        }
    }
    Err(CredentialError::NoneSet)
}

/// One variable: unset or empty → `None`; set but not UTF-8 → refused by name.
fn read(
    lookup: &dyn Fn(&str) -> Option<OsString>,
    name: &'static str,
) -> Result<Option<String>, CredentialError> {
    match lookup(name).map(OsString::into_string) {
        None => Ok(None),
        Some(Ok(value)) if value.is_empty() => Ok(None),
        Some(Ok(value)) => Ok(Some(value)),
        Some(Err(_)) => Err(CredentialError::NotUnicode(name)),
    }
}
