//! The capability matrix (proposal 0017 §3) and the failure taxonomy that judges it (§7).
//!
//! Pure: nothing here sends a request, opens a socket or reads a clock. A scenario maps what
//! came back onto this module's own [`Answer`] and [`Exercise`] and hands them to [`classify`]
//! and [`verdict`]; of the client's types only [`ErrorCode`] reaches it. A [`Profile`] declares
//! one of three states ([`Expected`]) per [`Operation`] and is selected by the server's version
//! ([`parse_version`], [`select`]).
//!
//! The invariant: **the verdict never claims more than the answers prove, and never less.**
//! Every exercise of every row goes through one ladder, [`classify`]: an exercise of another
//! row shows nothing about this one; an exercise whose own request got no response head has no
//! verdict; otherwise every check the row's evidence names runs on everything that arrived, and
//! the first one to fail, in message order, makes the result fatal. Only when none failed does
//! something that merely leaves the row unproved (a body cut off, a row the tool does not check)
//! make it unsettled. So an `unsupported` operation that succeeds in any part is
//! `unexpected-support`, an answer that is not the row's defined one is fatal (a 5xx included),
//! and a cut never hides a wrong status or byte that already arrived. A run with any row
//! unproved, or none proved, is INCONCLUSIVE, never PASS.
//!
//! A `required` row reads a closed table of facts: the defined status, the headers the table
//! names, the body or document, and the follow-up `HEAD`s of a delete. An exercise whose recorded
//! ask cannot test its row never passes, and is still judged by every check that does not depend
//! on the ask.

use std::cmp::Ordering;
use std::fmt;

use crate::s3::ErrorCode;

// --- Operations and rows --------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Operation {
    PutObject,
    /// `GetObject` of the whole object.
    GetObject,
    /// `GetObject` with `Range: bytes=a-b`.
    GetObjectRanged,
    /// `GetObject` with an `If-None-Match` naming the object's current entity-tag.
    GetObjectIfNoneMatchHit,
    /// `GetObject` with an `If-None-Match` naming a strong tag the object does not have.
    GetObjectIfNoneMatchMiss,
    HeadObject,
    DeleteObject,
    /// Bulk `POST /bucket?delete`.
    DeleteObjects,
    ListObjectsV2,
    /// The v1 listing (`GET /bucket` without `list-type=2`).
    ListObjects,
    CopyObject,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
    ListMultipartUploads,
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    ListBuckets,
    GetBucketLocation,
}

/// The error code an `unsupported` row declares: a `<Code>`, or no body at all (`HEAD`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredCode {
    Code(&'static str),
    NoBody,
}

impl DeclaredCode {
    /// A `<Code>` matches only that code; no body matches a response the client read no code
    /// from, however it said so (a code read means an `<Error>` document arrived).
    fn matches(self, observed: Option<&ErrorCode>) -> bool {
        match (self, observed) {
            (Self::Code(want), Some(ErrorCode::Code(got))) => want == got,
            (Self::Code(_), _) => false,
            (Self::NoBody, None | Some(ErrorCode::NoBody)) => true,
            (Self::NoBody, Some(ErrorCode::Code(_) | ErrorCode::MissingInXml)) => false,
        }
    }
}

impl fmt::Display for DeclaredCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Code(code) => f.write_str(code),
            Self::NoBody => f.write_str("no body"),
        }
    }
}

/// A row's declared state (proposal 0017 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// Must succeed, and the answer must show the operation was done.
    Required,
    /// Must be refused with exactly this status and code; `issue` will build the operation.
    Unsupported {
        issue: u32,
        status: u16,
        code: DeclaredCode,
    },
    /// Expected to be accepted; the tool cannot check it until `issue` lands. Never a pass.
    SupportedNotChecked { issue: u32 },
}

impl fmt::Display for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Required => f.write_str("required"),
            Self::Unsupported {
                issue,
                status,
                code,
            } => write!(f, "unsupported(#{issue}), refused {status} {code}"),
            Self::SupportedNotChecked { issue } => write!(f, "supported, not checked (#{issue})"),
        }
    }
}

/// `cites` is the source line that gives the answer the row expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Row {
    pub operation: Operation,
    pub expected: Expected,
    pub cites: &'static str,
}

impl Row {
    pub const fn required(operation: Operation, cites: &'static str) -> Self {
        Self {
            operation,
            expected: Expected::Required,
            cites,
        }
    }

    pub const fn unsupported(
        operation: Operation,
        issue: u32,
        status: u16,
        code: DeclaredCode,
        cites: &'static str,
    ) -> Self {
        Self {
            operation,
            expected: Expected::Unsupported {
                issue,
                status,
                code,
            },
            cites,
        }
    }

    pub const fn not_checked(operation: Operation, issue: u32, cites: &'static str) -> Self {
        Self {
            operation,
            expected: Expected::SupportedNotChecked { issue },
            cites,
        }
    }

    /// Whether a run must prove the row: `required` and `unsupported` rows, not a
    /// `supported, not checked` one.
    fn must_prove(&self) -> bool {
        !matches!(self.expected, Expected::SupportedNotChecked { .. })
    }
}

// --- Profiles -------------------------------------------------------------------------------

/// The edge of a profile's range: a release with no pre-release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl Bound {
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

/// At or above `from` and below `below` (each when set), by SemVer precedence, so
/// `0.1.0-rc.1` is below `0.1.0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionRange {
    pub from: Option<Bound>,
    pub below: Option<Bound>,
}

impl VersionRange {
    pub fn contains(&self, release: &Release) -> bool {
        let at_or_above = self
            .from
            .is_none_or(|from| release.cmp_bound(from) != Ordering::Less);
        let under = self
            .below
            .is_none_or(|below| release.cmp_bound(below) == Ordering::Less);
        at_or_above && under
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Profile {
    name: &'static str,
    range: VersionRange,
    rows: &'static [Row],
}

impl Profile {
    /// Panics if two rows declare one operation (`row` would hide the second); in a `const`
    /// that is a compile error.
    pub const fn new(name: &'static str, range: VersionRange, rows: &'static [Row]) -> Self {
        let mut i = 0;
        while i < rows.len() {
            let mut j = i + 1;
            while j < rows.len() {
                if rows[i].operation as usize == rows[j].operation as usize {
                    panic!("a profile declares an operation twice");
                }
                j += 1;
            }
            i += 1;
        }
        Self { name, range, rows }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn rows(&self) -> &'static [Row] {
        self.rows
    }

    pub fn row(&self, operation: Operation) -> Option<&'static Row> {
        self.rows.iter().find(|row| row.operation == operation)
    }
}

/// A citation of `crates/gateway-s3/src/lib.rs:<lines>`.
macro_rules! gw {
    ($lines:literal) => {
        concat!("crates/gateway-s3/src/lib.rs:", $lines)
    };
}

/// `unsupported`: refused `501 NotImplemented`.
const fn nimpl(operation: Operation, issue: u32, cites: &'static str) -> Row {
    Row::unsupported(
        operation,
        issue,
        501,
        DeclaredCode::Code("NotImplemented"),
        cites,
    )
}

/// `unsupported(#511)`: a bucket operation refused `400 InvalidRequest`.
const fn invalid(operation: Operation, cites: &'static str) -> Row {
    Row::unsupported(
        operation,
        511,
        400,
        DeclaredCode::Code("InvalidRequest"),
        cites,
    )
}

use Operation as Op;

/// The multipart and `?location` refusals come from the subresource denylist at `:410-422`.
/// Until the first `0.1.0` release this profile describes `main` and is edited in place (0017
/// §3; every untagged build reports `0.0.0+git.<sha>`, so no second pre-release profile could
/// be selected). When #508 lands the six multipart rows move to `supported, not checked (#750)`.
const PRE_0_1_ROWS: &[Row] = &[
    Row::required(Op::PutObject, gw!("2008 (put_object_response :3401-3404)")),
    Row::required(Op::GetObject, gw!("2017 (serve_get :2469)")),
    Row::required(
        Op::GetObjectRanged,
        gw!("2704-2725 (206, Content-Length and Content-Range)"),
    ),
    Row::required(
        Op::GetObjectIfNoneMatchHit,
        gw!("2785-2803 (304; If-None-Match :2995)"),
    ),
    Row::required(
        Op::GetObjectIfNoneMatchMiss,
        gw!("2995 (no match; serve_get :2017)"),
    ),
    Row::required(Op::HeadObject, gw!("2025 (serve_head :2570)")),
    Row::required(Op::DeleteObject, gw!("2026-2028 (204)")),
    Row::required(
        Op::DeleteObjects,
        gw!("1766-1788 (POST ?delete); errors :2293-2300"),
    ),
    Row::required(Op::ListObjectsV2, gw!("1806-1808 (list-type=2 :868)")),
    Row::required(Op::ListObjects, gw!("1806-1808 (v1 shim :855)")),
    nimpl(Op::CopyObject, 766, gw!("1865-1871 (x-amz-copy-source)")),
    nimpl(Op::CreateMultipartUpload, 508, gw!("1842-1849 (?uploads)")),
    nimpl(Op::UploadPart, 508, gw!("1842-1849 (?partNumber&uploadId)")),
    nimpl(
        Op::CompleteMultipartUpload,
        508,
        gw!("1842-1849 (?uploadId)"),
    ),
    nimpl(Op::AbortMultipartUpload, 508, gw!("1842-1849 (?uploadId)")),
    nimpl(Op::ListParts, 508, gw!("1842-1849 (?uploadId)")),
    nimpl(
        Op::ListMultipartUploads,
        508,
        gw!("1797-1804 (bucket ?uploads)"),
    ),
    nimpl(
        Op::GetBucketLocation,
        511,
        gw!("1797-1804 (bucket ?location)"),
    ),
    invalid(Op::CreateBucket, gw!("1809-1815 (bucket path, PUT)")),
    invalid(Op::DeleteBucket, gw!("1809-1815 (bucket path, DELETE)")),
    invalid(Op::ListBuckets, gw!("1819-1826 (root path /)")),
    Row::unsupported(
        Op::HeadBucket,
        511,
        400,
        DeclaredCode::NoBody,
        gw!("1809-1815 (bucket path, HEAD; no body on a HEAD :1548-1553)"),
    ),
];

/// Every Wyrd build below `0.1.0`: today's gateway.
pub const PRE_0_1: Profile = Profile::new(
    "pre-0.1",
    VersionRange {
        from: None,
        below: Some(Bound::new(0, 1, 0)),
    },
    PRE_0_1_ROWS,
);

/// The profiles this tool ships, one per release range. The ranges do not overlap.
pub const PROFILES: &[Profile] = &[PRE_0_1];

// --- Versions -------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreId {
    Numeric(u64),
    Alpha(String),
}

/// `MAJOR.MINOR.PATCH` with any pre-release; build metadata never changes the selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub pre: Vec<PreId>,
}

impl Release {
    /// On the same triple a pre-release sorts before the release (SemVer 2.0.0 §11).
    fn cmp_bound(&self, bound: Bound) -> Ordering {
        (self.major, self.minor, self.patch)
            .cmp(&(bound.major, bound.minor, bound.patch))
            .then(if self.pre.is_empty() {
                Ordering::Equal
            } else {
                Ordering::Less
            })
    }
}

impl fmt::Display for Release {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        for (i, id) in self.pre.iter().enumerate() {
            f.write_str(if i == 0 { "-" } else { "." })?;
            match id {
                PreId::Numeric(n) => write!(f, "{n}")?,
                PreId::Alpha(s) => f.write_str(s)?,
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportedVersion {
    Release(Release),
    /// The gateway's default `unknown` (`crates/gateway-s3/src/lib.rs:121`) or the "nothing was
    /// known" identity `+git.unknown[.dirty]` (`crates/server/src/version/derivation.rs:65`).
    /// Never read as `0.0.0`.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionError {
    pub input: String,
    pub reason: &'static str,
}

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot read version {:?}: {}", self.input, self.reason)
    }
}

impl std::error::Error for VersionError {}

const UNKNOWN: &str = "unknown";

const UNKNOWN_BUILDS: [&str; 2] = ["git.unknown", "git.unknown.dirty"];

/// `unknown`, or `MAJOR.MINOR.PATCH[-PRE][+BUILD]` by the SemVer 2.0.0 grammar, every token
/// checked. The server emits `0.0.0+git.<sha>[.dirty]`, `0.1.0`, `0.1.0+git.dirty` and
/// `0.1.0+git.3.<sha>[.dirty]` (`crates/server/src/version/derivation.rs:33-68`).
pub fn parse_version(input: &str) -> Result<ReportedVersion, VersionError> {
    let err = |reason| VersionError {
        input: input.to_string(),
        reason,
    };
    if input == UNKNOWN {
        return Ok(ReportedVersion::Unknown);
    }
    let (release_part, build) = match input.split_once('+') {
        Some((release, build)) => (release, Some(build)),
        None => (input, None),
    };
    let ident = |id: &str| !id.is_empty() && id.bytes().all(is_ident_byte);
    if build.is_some_and(|build| !build.split('.').all(ident)) {
        return Err(err("malformed build metadata"));
    }
    let (core, pre) = match release_part.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (release_part, None),
    };
    let mut numbers = core.split('.');
    let (Some(major), Some(minor), Some(patch), None) = (
        numbers.next(),
        numbers.next(),
        numbers.next(),
        numbers.next(),
    ) else {
        return Err(err("expected MAJOR.MINOR.PATCH"));
    };
    let number = |s: &str| parse_numeric(s).ok_or_else(|| err("malformed version number"));
    let (major, minor, patch) = (number(major)?, number(minor)?, number(patch)?);
    let mut pre_ids = Vec::new();
    if let Some(pre) = pre {
        for id in pre.split('.') {
            if !ident(id) {
                return Err(err("malformed pre-release"));
            }
            if id.bytes().all(|b| b.is_ascii_digit()) {
                pre_ids.push(PreId::Numeric(
                    parse_numeric(id).ok_or_else(|| err("malformed pre-release"))?,
                ));
            } else {
                pre_ids.push(PreId::Alpha(id.to_string()));
            }
        }
    }
    if build.is_some_and(|build| UNKNOWN_BUILDS.contains(&build)) {
        return Ok(ReportedVersion::Unknown);
    }
    Ok(ReportedVersion::Release(Release {
        major,
        minor,
        patch,
        pre: pre_ids,
    }))
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-'
}

/// ASCII digits only (no `+` sign, which `u64::from_str` takes), no leading zero, fits `u64`.
fn parse_numeric(s: &str) -> Option<u64> {
    if !s.bytes().all(|b| b.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0')) {
        return None;
    }
    s.parse().ok()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoProfile {
    Unknown,
    Unparsable(VersionError),
    NoMatch(Release),
    /// More than one profile covers it: picking one would be a guess.
    Ambiguous(Release),
}

impl fmt::Display for NoProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("the server's version is unknown"),
            Self::Unparsable(e) => write!(f, "{e}"),
            Self::NoMatch(r) => write!(f, "no profile covers release {r}"),
            Self::Ambiguous(r) => write!(f, "more than one profile covers release {r}"),
        }
    }
}

pub fn select<'a>(
    profiles: &'a [Profile],
    version: &ReportedVersion,
) -> Result<&'a Profile, NoProfile> {
    let release = match version {
        ReportedVersion::Unknown => return Err(NoProfile::Unknown),
        ReportedVersion::Release(release) => release,
    };
    let mut covering = profiles.iter().filter(|p| p.range.contains(release));
    match (covering.next(), covering.next()) {
        (Some(profile), None) => Ok(profile),
        (None, _) => Err(NoProfile::NoMatch(release.clone())),
        (Some(_), Some(_)) => Err(NoProfile::Ambiguous(release.clone())),
    }
}

/// [`parse_version`] then [`select`].
pub fn select_reported<'a>(profiles: &'a [Profile], input: &str) -> Result<&'a Profile, NoProfile> {
    let version = parse_version(input).map_err(NoProfile::Unparsable)?;
    select(profiles, &version)
}

// --- Observations ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// No response head arrived: no connection, a reset, a timeout before the head.
    NoAnswer {
        why: String,
    },
    Response(Response),
}

/// A response head and the body that followed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub etag: Option<String>,
    pub content_range: Option<String>,
    /// The declared `Content-Length`, when the response carried one.
    pub content_length: Option<u64>,
    /// The S3 error code of an error response, as the client read it; `None` on a success.
    pub error_code: Option<ErrorCode>,
    pub body: Body,
}

impl Response {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            etag: None,
            content_range: None,
            content_length: None,
            error_code: None,
            body: Body::Empty,
        }
    }

    pub fn with_etag(mut self, etag: &str) -> Self {
        self.etag = Some(etag.to_string());
        self
    }

    pub fn with_content_range(mut self, content_range: &str) -> Self {
        self.content_range = Some(content_range.to_string());
        self
    }

    pub fn with_content_length(mut self, content_length: u64) -> Self {
        self.content_length = Some(content_length);
        self
    }

    pub fn with_error_code(mut self, code: ErrorCode) -> Self {
        self.error_code = Some(code);
        self
    }

    pub fn with_body(mut self, body: Body) -> Self {
        self.body = body;
        self
    }

    fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Empty,
    Bytes(Vec<u8>),
    /// A listing document, every page together: each key with its size.
    Listing(Vec<Listed>),
    /// A `<DeleteResult>` document (requested without `Quiet`).
    DeleteResult(DeleteResult),
    /// Arrived whole, and is not the document its status calls for (an error body that is not
    /// one S3 `<Error>` document; a 200 without its listing or `<DeleteResult>`): `protocol`.
    Unreadable {
        why: String,
    },
    /// The head arrived and the body was then cut off (a transport failure, a timeout) after
    /// `arrived`. What arrived is evidence; the cut itself settles nothing.
    Cut {
        arrived: Arrived,
        why: String,
    },
}

/// What arrived before a cut: the bytes (a streamed object body), or only how many (a body the
/// client buffers whole).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arrived {
    Bytes(Vec<u8>),
    Count(u64),
}

impl Arrived {
    fn count(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::Count(count) => *count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Listed {
    pub key: String,
    pub size: u64,
}

/// The keys under `<Deleted>`, and each `<Error>` row.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteResult {
    pub deleted: Vec<String>,
    pub errors: Vec<DeleteFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFailure {
    pub key: String,
    pub code: String,
}

/// One exercise of one row: what it asked (as recorded), and every answer the row's checks
/// read, in message order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exercise {
    PutObject {
        answer: Answer,
        /// Whether the request body had ended when the response head arrived. A success
        /// before that is not a receipt (the client's `BodyError::AcknowledgedEarly`).
        request_body_ended: bool,
    },
    GetObject {
        object: Vec<u8>,
        answer: Answer,
    },
    /// `Range: bytes=first-last` of `object`.
    GetObjectRanged {
        object: Vec<u8>,
        first: u64,
        last: u64,
        answer: Answer,
    },
    /// `If-None-Match: sent` of `object`, whose PUT answered `ETag: etag`. Fit to test its
    /// row only when `sent` is a strong entity-tag equal to `etag`.
    GetObjectIfNoneMatchHit {
        sent: String,
        etag: String,
        object: Vec<u8>,
        answer: Answer,
    },
    /// `If-None-Match: sent` of `object`, whose PUT answered `ETag: etag`. Fit to test its
    /// row only when `sent` is a strong entity-tag that differs from `etag`: the negative
    /// control that tells "compares tags" from "answers 304 to anything".
    GetObjectIfNoneMatchMiss {
        sent: String,
        etag: String,
        object: Vec<u8>,
        answer: Answer,
    },
    /// `size` and `etag` are the object's: its length and the `ETag` its PUT answered.
    HeadObject {
        size: u64,
        etag: String,
        answer: Answer,
    },
    /// The delete of `key`. `follow_ups` are the `HEAD`s sent after the delete answered, each
    /// with the key it asked about and its own answer or no answer; a key may be probed more
    /// than once (a retry).
    DeleteObject {
        key: String,
        answer: Answer,
        follow_ups: Vec<(String, Answer)>,
    },
    /// The bulk delete of `keys`. `follow_ups` as for [`Exercise::DeleteObject`].
    DeleteObjects {
        keys: Vec<String>,
        answer: Answer,
        follow_ups: Vec<(String, Answer)>,
    },
    /// `expected` is every key written under the listed prefix, with its size.
    ListObjectsV2 {
        expected: Vec<Listed>,
        answer: Answer,
    },
    ListObjects {
        expected: Vec<Listed>,
        answer: Answer,
    },
    /// An operation exercised with nothing recorded but its answer (an `unsupported` row's
    /// refusal): no check of a `required` row can read it.
    Other {
        operation: Operation,
        answer: Answer,
    },
}

impl Exercise {
    pub fn operation(&self) -> Operation {
        match self {
            Self::PutObject { .. } => Operation::PutObject,
            Self::GetObject { .. } => Operation::GetObject,
            Self::GetObjectRanged { .. } => Operation::GetObjectRanged,
            Self::GetObjectIfNoneMatchHit { .. } => Operation::GetObjectIfNoneMatchHit,
            Self::GetObjectIfNoneMatchMiss { .. } => Operation::GetObjectIfNoneMatchMiss,
            Self::HeadObject { .. } => Operation::HeadObject,
            Self::DeleteObject { .. } => Operation::DeleteObject,
            Self::DeleteObjects { .. } => Operation::DeleteObjects,
            Self::ListObjectsV2 { .. } => Operation::ListObjectsV2,
            Self::ListObjects { .. } => Operation::ListObjects,
            Self::Other { operation, .. } => *operation,
        }
    }

    /// The answer to the exercise's own request (not a follow-up's).
    pub fn answer(&self) -> &Answer {
        match self {
            Self::PutObject { answer, .. }
            | Self::GetObject { answer, .. }
            | Self::GetObjectRanged { answer, .. }
            | Self::GetObjectIfNoneMatchHit { answer, .. }
            | Self::GetObjectIfNoneMatchMiss { answer, .. }
            | Self::HeadObject { answer, .. }
            | Self::DeleteObject { answer, .. }
            | Self::DeleteObjects { answer, .. }
            | Self::ListObjectsV2 { answer, .. }
            | Self::ListObjects { answer, .. }
            | Self::Other { answer, .. } => answer,
        }
    }
}

// --- Classification -------------------------------------------------------------------------

/// The fatal classes of proposal 0017 §7. `availability` is not one: it covers only failures
/// that leave no answer to judge; a 5xx is an answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Bytes, digest or size differ.
    Integrity,
    /// The model and the server disagree (a deleted key readable, a written key not found, a
    /// listing mismatch).
    Oracle,
    /// An `unsupported` operation succeeded.
    UnexpectedSupport,
    /// Wrong status (a 5xx included), header, error code, or document shape.
    Protocol,
}

impl fmt::Display for FailureClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Integrity => "integrity",
            Self::Oracle => "oracle",
            Self::UnexpectedSupport => "unexpected-support",
            Self::Protocol => "protocol",
        })
    }
}

/// What one exercise shows about its row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowResult {
    Pass,
    Fatal {
        class: FailureClass,
        detail: String,
    },
    /// No verdict: the exercise's own request got no response head.
    NoAnswer {
        why: String,
    },
    /// Nothing proved the answer wrong, and something left the row unproved: a body cut off,
    /// an exercise of another row, an exercise its row has no check for.
    Unsettled {
        why: String,
    },
    /// The row is `supported, not checked (#issue)`: nothing proved the answer wrong, and it
    /// is not a pass.
    NotChecked {
        issue: u32,
    },
}

/// What one check found on one exercise. A check that passed finds nothing.
#[derive(Debug)]
enum Finding {
    /// A fact that proves the answer wrong.
    Fatal(FailureClass, String),
    /// Something that leaves the row unproved and proves nothing wrong.
    Unproved(String),
    /// The row is `supported, not checked (#issue)`.
    NotChecked(u32),
}

impl Finding {
    fn protocol(detail: String) -> Self {
        Self::Fatal(FailureClass::Protocol, detail)
    }
}

/// Everything the checks of one exercise found, kept by where in the message each fact comes
/// from. A check adds to the stage its evidence comes from and never returns early, so every
/// check runs on everything that arrived; `settle` then reads the stages in message order.
#[derive(Debug, Default)]
struct Evidence {
    /// What the exercise asked, as recorded.
    request: Vec<Finding>,
    /// The status of the exercise's own response head.
    status: Vec<Finding>,
    /// The headers, in the order of the row's table.
    headers: Vec<Finding>,
    /// The body: whole, or cut, with what arrived before the cut.
    body: Vec<Finding>,
    /// Each follow-up request's own answer, in their order.
    follow_ups: Vec<Finding>,
    /// What leaves the row unproved whatever arrived: a row the tool does not check, or an
    /// exercise that carries no check of its own.
    open: Vec<Finding>,
}

impl Evidence {
    /// Steps 3 to 5 of the ladder. The first fatal finding in message order is the result
    /// (request, status, headers, body, follow-ups); only if there is none does the first
    /// finding that left the row unproved decide; with neither, the exercise passed.
    fn settle(self) -> RowResult {
        let in_message_order = [
            self.request,
            self.status,
            self.headers,
            self.body,
            self.follow_ups,
            self.open,
        ];
        let mut unproved = None;
        for finding in in_message_order.into_iter().flatten() {
            match finding {
                Finding::Fatal(class, detail) => return RowResult::Fatal { class, detail },
                Finding::Unproved(why) => {
                    unproved.get_or_insert(RowResult::Unsettled { why });
                }
                Finding::NotChecked(issue) => {
                    unproved.get_or_insert(RowResult::NotChecked { issue });
                }
            }
        }
        unproved.unwrap_or(RowResult::Pass)
    }
}

/// Judge one exercise of `row`, by the one ladder every row goes through:
///
/// 1. an exercise of another row shows nothing about this one: unsettled;
/// 2. the exercise's own request got no response head: no verdict (its follow-ups are not
///    judged);
/// 3. every check the row's evidence names runs on everything that arrived, and if any
///    failed the result is fatal, with the class of the first to fail in message order;
/// 4. unsettled, only when nothing failed and something left the row unproved;
/// 5. pass.
pub fn classify(row: &Row, exercise: &Exercise) -> RowResult {
    let (of, want) = (exercise.operation(), row.operation);
    if of != want {
        return RowResult::Unsettled {
            why: format!("an exercise of {of:?} cannot settle the {want:?} row"),
        };
    }
    let response = match exercise.answer() {
        Answer::NoAnswer { why } => return RowResult::NoAnswer { why: why.clone() },
        Answer::Response(response) => response,
    };
    let mut evidence = Evidence::default();
    shared_facts(row, response, &mut evidence);
    match row.expected {
        Expected::Unsupported { status, code, .. } => {
            refusal(status, code, response, &mut evidence)
        }
        Expected::SupportedNotChecked { issue } => {
            evidence.open.push(Finding::NotChecked(issue));
        }
        Expected::Required => required(exercise, response, &mut evidence),
    }
    evidence.settle()
}

/// The fact every exercise that got a head is held to, whatever its row and whatever it asked:
/// a 5xx is an answer, and not the defined one, so it is `protocol` unless the row declares
/// that very status (0017 §7). (Its pair, a 404 to a typed read of a written key being
/// `oracle`, reads the `required` rows' exercises: see [`required`].)
fn shared_facts(row: &Row, response: &Response, evidence: &mut Evidence) {
    let got = response.status;
    let declared = matches!(row.expected, Expected::Unsupported { status, .. } if status == got);
    if (500..=599).contains(&got) && !declared {
        let expected = row.expected;
        evidence.status.push(Finding::protocol(format!(
            "answered {got}, a 5xx the row does not declare; the row is {expected}"
        )));
    }
}

/// An `unsupported` row reads the status and the body, and nothing else (no header, no
/// `Content-Length`). The status: any 2xx is support, whatever the body did after; any other
/// status but the declared one is `protocol`. The body: one S3 `<Error>` document with the
/// declared code, or, for a row declaring no body, no byte at all.
fn refusal(status: u16, code: DeclaredCode, response: &Response, evidence: &mut Evidence) {
    let got = response.status;
    if response.is_success() {
        evidence.status.push(Finding::Fatal(
            FailureClass::UnexpectedSupport,
            format!("answered {got}; declared {status} {code}"),
        ));
    } else if got != status {
        evidence.status.push(Finding::protocol(format!(
            "answered {got}; declared {status} {code}"
        )));
    }
    let body = &mut evidence.body;
    match (code, &response.body) {
        (_, Body::Empty) => {}
        (DeclaredCode::NoBody, Body::Bytes(bytes)) if bytes.is_empty() => {}
        (DeclaredCode::NoBody, Body::Bytes(bytes)) => body.push(Finding::protocol(format!(
            "answered {got} with a body of {} bytes",
            bytes.len()
        ))),
        (DeclaredCode::NoBody, Body::Cut { arrived, why }) => match arrived.count() {
            0 => body.push(Finding::Unproved(format!(
                "{got} cut off after the head: {why}"
            ))),
            n => body.push(Finding::protocol(format!(
                "answered {got} with a body of {n} bytes"
            ))),
        },
        (
            DeclaredCode::NoBody,
            Body::Unreadable { .. } | Body::Listing(_) | Body::DeleteResult(_),
        ) => body.push(Finding::protocol(format!("answered {got} with a body"))),
        (DeclaredCode::Code(_), Body::Bytes(_)) => {}
        (DeclaredCode::Code(_), Body::Cut { why, .. }) => body.push(Finding::Unproved(format!(
            "{got} error body cut off before its code could be read: {why}"
        ))),
        (DeclaredCode::Code(_), Body::Unreadable { why }) => body.push(Finding::protocol(format!(
            "{got} error body is not an <Error> document: {why}"
        ))),
        (DeclaredCode::Code(_), Body::Listing(_) | Body::DeleteResult(_)) => body.push(
            Finding::protocol(format!("{got} error body is not an <Error> document")),
        ),
    }
    // The code, whenever one could be read: a cut that left it unread proves nothing about it,
    // and a cut never hides a wrong one that was read.
    let unread = matches!(response.body, Body::Cut { .. }) && response.error_code.is_none();
    if !unread && !code.matches(response.error_code.as_ref()) {
        let observed = match &response.error_code {
            Some(observed) => observed.to_string(),
            None => "no error code".to_string(),
        };
        body.push(Finding::protocol(format!(
            "answered {got} with {observed}; declared {status} {code}"
        )));
    }
}

/// A `required` row's own evidence, by what its exercise asked and recorded. The table is closed:
/// a row reads these facts and no others.
///
/// | Row | Status | Headers, in this order | Body |
/// |---|---|---|---|
/// | `PutObject` | 200 | `ETag` strong; `Content-Length` absent or 0 | none, after the request body ended |
/// | `GetObject` whole | 200 | `Content-Length` absent or the size | the object's bytes |
/// | ranged | 206 | `Content-Range` exact; `Content-Length` absent or the span | the span's bytes |
/// | conditional, matching | 304 | none | none |
/// | conditional, non-matching | 200 | as whole `GetObject` | the object's bytes |
/// | `HeadObject` | 200 | `Content-Length` = size; `ETag` = the PUT's | none |
/// | `DeleteObject` | 204 | `Content-Length` absent or 0 | none, then the follow-up `HEAD`s |
/// | `DeleteObjects` | 200 | none | a `<DeleteResult>`, then the follow-up `HEAD`s |
/// | follow-up `HEAD` of a requested key | 404 | none | none |
/// | `ListObjectsV2`, v1 | 200 | none | the listing, every page together |
///
/// No check returns early and none is skipped for a cut or an unfit ask, except that a ranged GET
/// outside its object and a `DeleteObjects` of no keys read nothing of their own answer past the
/// two facts every exercise is held to: what they asked says nothing about what the rest should
/// be. A check after a wrong status cannot change the result (the status comes first in message
/// order), so those checks do not look at the status themselves.
fn required(exercise: &Exercise, response: &Response, evidence: &mut Evidence) {
    if is_typed_read(exercise) && response.status == 404 {
        evidence.status.push(Finding::Fatal(
            FailureClass::Oracle,
            "answered 404 to a read of a key the run wrote".to_string(),
        ));
    }
    match exercise {
        Exercise::PutObject {
            request_body_ended, ..
        } => {
            // A success before that is not a receipt (`BodyError::AcknowledgedEarly`); a refusal
            // may rightly come early, and a wrong status is judged below.
            if response.is_success() && !request_body_ended {
                let got = response.status;
                evidence.request.push(Finding::protocol(format!(
                    "answered {got} before the request body had ended"
                )));
            }
            defined_status(response, 200, evidence);
            strong_etag(response, evidence);
            declares_no_body(response, evidence);
            no_body(response, &mut evidence.body);
        }
        Exercise::GetObject { object, .. } => {
            defined_status(response, 200, evidence);
            object_bytes(response, object, evidence);
        }
        Exercise::GetObjectRanged {
            object,
            first,
            last,
            ..
        } => {
            let size = object.len() as u64;
            if first > last || *last >= size {
                evidence.request.push(Finding::Unproved(format!(
                    "asked bytes={first}-{last} of an object of {size} bytes"
                )));
                return;
            }
            defined_status(response, 206, evidence);
            content_range(response, (*first, *last, size), evidence);
            // In range by the guard above, so the slice cannot panic.
            object_bytes(
                response,
                &object[*first as usize..=*last as usize],
                evidence,
            );
        }
        Exercise::GetObjectIfNoneMatchHit {
            sent, etag, object, ..
        } => {
            let fit = is_strong_etag(sent) && sent == etag;
            conditional(fit, 304, sent, etag, object, response, evidence);
        }
        Exercise::GetObjectIfNoneMatchMiss {
            sent, etag, object, ..
        } => {
            let fit = is_strong_etag(sent) && sent != etag;
            conditional(fit, 200, sent, etag, object, response, evidence);
        }
        Exercise::HeadObject { size, etag, .. } => {
            defined_status(response, 200, evidence);
            head_matches_the_object(response, *size, etag, evidence);
            no_body(response, &mut evidence.body);
        }
        Exercise::DeleteObject {
            key, follow_ups, ..
        } => {
            defined_status(response, 204, evidence);
            declares_no_body(response, evidence);
            no_body(response, &mut evidence.body);
            judge_follow_ups(std::slice::from_ref(key), follow_ups, evidence);
        }
        Exercise::DeleteObjects {
            keys, follow_ups, ..
        } => {
            if keys.is_empty() {
                evidence.request.push(Finding::Unproved(
                    "a DeleteObjects of no keys tests nothing".to_string(),
                ));
            } else {
                let mut requested = keys.clone();
                requested.sort();
                let named_twice = requested.windows(2).any(|pair| pair[0] == pair[1]);
                if named_twice {
                    evidence.request.push(Finding::Unproved(format!(
                        "the delete names a key twice: {requested:?}"
                    )));
                }
                defined_status(response, 200, evidence);
                delete_result(
                    response,
                    (!named_twice).then_some(requested.as_slice()),
                    &mut evidence.body,
                );
            }
            // Asked of no keys, every follow-up is of a key the delete did not request.
            judge_follow_ups(keys, follow_ups, evidence);
        }
        Exercise::ListObjectsV2 { expected, .. } | Exercise::ListObjects { expected, .. } => {
            if expected.is_empty() {
                evidence.request.push(Finding::Unproved(
                    "a listing of a prefix with nothing written proves nothing".to_string(),
                ));
            }
            defined_status(response, 200, evidence);
            listing(response, expected, &mut evidence.body);
        }
        Exercise::Other { operation, .. } => evidence.open.push(Finding::Unproved(format!(
            "this exercise of {operation:?} carries no check of a required row"
        ))),
    }
}

/// A `GetObject` form's or `HeadObject`'s own request, of a key the run wrote. A delete's
/// follow-up `HEAD` is not one: there a 404 is the defined answer.
fn is_typed_read(exercise: &Exercise) -> bool {
    matches!(
        exercise,
        Exercise::GetObject { .. }
            | Exercise::GetObjectRanged { .. }
            | Exercise::GetObjectIfNoneMatchHit { .. }
            | Exercise::GetObjectIfNoneMatchMiss { .. }
            | Exercise::HeadObject { .. }
    )
}

fn defined_status(response: &Response, defined: u16, evidence: &mut Evidence) {
    let got = response.status;
    if got != defined {
        evidence.status.push(Finding::protocol(format!(
            "answered {got}; the row defines {defined}"
        )));
    }
}

/// The `If-None-Match` forms. An unfit ask (`fit` false) cannot tell which answer is the defined
/// one, so 200 and 304 are both acceptable and the row stays unproved. Either way a 200 carries
/// the object and a 304 carries nothing; any other status is already fatal.
fn conditional(
    fit: bool,
    defined: u16,
    sent: &str,
    etag: &str,
    object: &[u8],
    response: &Response,
    evidence: &mut Evidence,
) {
    if fit {
        defined_status(response, defined, evidence);
    } else {
        evidence.request.push(Finding::Unproved(format!(
            "sent If-None-Match: {sent}, which cannot test this row of an object tagged {etag}"
        )));
        let got = response.status;
        if !matches!(got, 200 | 304) {
            evidence.status.push(Finding::protocol(format!(
                "answered {got}; a conditional GET answers 200 or 304"
            )));
        }
    }
    match response.status {
        200 => object_bytes(response, object, evidence),
        304 => no_body(response, &mut evidence.body),
        _ => {}
    }
}

fn strong_etag(response: &Response, evidence: &mut Evidence) {
    match response.etag.as_deref() {
        Some(etag) if is_strong_etag(etag) => {}
        Some(etag) => evidence.headers.push(Finding::protocol(format!(
            "ETag {etag:?} is not a strong entity-tag"
        ))),
        None => evidence
            .headers
            .push(Finding::protocol("answered with no ETag".to_string())),
    }
}

/// A non-zero declared length declares a body the answer must not have, whatever arrived after.
fn declares_no_body(response: &Response, evidence: &mut Evidence) {
    if let Some(declared) = response.content_length.filter(|&declared| declared != 0) {
        let got = response.status;
        evidence.headers.push(Finding::protocol(format!(
            "answered {got} declaring Content-Length {declared}; it carries no body"
        )));
    }
}

fn content_range(response: &Response, want: (u64, u64, u64), evidence: &mut Evidence) {
    let (first, last, size) = want;
    match response.content_range.as_deref() {
        Some(value) if parse_content_range(value) == Some(want) => {}
        Some(value) => evidence.headers.push(Finding::protocol(format!(
            "Content-Range {value:?}; expected \"bytes {first}-{last}/{size}\""
        ))),
        None => evidence.headers.push(Finding::protocol(
            "answered with no Content-Range".to_string(),
        )),
    }
}

/// What `HeadObject` reports is the server's account of the object: both are `oracle`, and an
/// absent header counts as different.
fn head_matches_the_object(response: &Response, size: u64, etag: &str, evidence: &mut Evidence) {
    let (length, tag) = (response.content_length, response.etag.as_deref());
    if length != Some(size) {
        evidence.headers.push(Finding::Fatal(
            FailureClass::Oracle,
            format!("Content-Length {length:?}; the object holds {size} bytes"),
        ));
    }
    if tag != Some(etag) {
        evidence.headers.push(Finding::Fatal(
            FailureClass::Oracle,
            format!("ETag {tag:?}; the PUT answered {etag:?}"),
        ));
    }
}

/// An answer that carries object bytes, `want` being those it must carry: the declared length,
/// then the bytes. A wrong byte is `integrity` whether or not a cut followed it; a correct prefix
/// or a count that still fits proves nothing either way.
fn object_bytes(response: &Response, want: &[u8], evidence: &mut Evidence) {
    let size = want.len() as u64;
    if let Some(declared) = response.content_length.filter(|&declared| declared != size) {
        evidence.headers.push(Finding::Fatal(
            FailureClass::Integrity,
            format!("Content-Length {declared}; expected {size} bytes"),
        ));
    }
    let body = &mut evidence.body;
    let differ = |got: &[u8], before: &str| {
        let n = got.len();
        let at = got
            .iter()
            .zip(want)
            .position(|(a, b)| a != b)
            .unwrap_or(got.len().min(want.len()));
        Finding::Fatal(
            FailureClass::Integrity,
            format!("{n} bytes{before} differ from the expected {size} at offset {at}"),
        )
    };
    match &response.body {
        Body::Empty if want.is_empty() => {}
        Body::Empty => body.push(differ(&[], "")),
        Body::Bytes(got) if got == want => {}
        Body::Bytes(got) => body.push(differ(got, "")),
        Body::Cut {
            arrived: Arrived::Bytes(got),
            why,
        } if want.starts_with(got) => body.push(Finding::Unproved(format!(
            "cut off after {} correct bytes of {size}: {why}",
            got.len()
        ))),
        Body::Cut {
            arrived: Arrived::Bytes(got),
            ..
        } => body.push(differ(got, " before the cut")),
        Body::Cut {
            arrived: Arrived::Count(count),
            why,
        } if *count <= size => body.push(Finding::Unproved(format!(
            "cut off after {count} bytes of {size}: {why}"
        ))),
        Body::Cut {
            arrived: Arrived::Count(count),
            ..
        } => body.push(Finding::Fatal(
            FailureClass::Integrity,
            format!("{count} bytes before the cut; expected {size}"),
        )),
        Body::Unreadable { why } => {
            body.push(Finding::protocol(format!("not object bytes: {why}")))
        }
        Body::Listing(_) | Body::DeleteResult(_) => body.push(Finding::protocol(
            "a document where object bytes were expected".to_string(),
        )),
    }
}

/// An answer that carries no body: any byte, before a cut or not, or any document is `protocol`;
/// a cut after no byte leaves the end of the answer unseen.
fn no_body(response: &Response, stage: &mut Vec<Finding>) {
    let got = response.status;
    match &response.body {
        Body::Empty => {}
        Body::Bytes(bytes) if bytes.is_empty() => {}
        Body::Bytes(bytes) => stage.push(Finding::protocol(format!(
            "answered {got} with a body of {} bytes",
            bytes.len()
        ))),
        Body::Cut { arrived, why } => match arrived.count() {
            0 => stage.push(Finding::Unproved(format!(
                "{got} cut off after the head: {why}"
            ))),
            n => stage.push(Finding::protocol(format!(
                "answered {got} with a body of {n} bytes"
            ))),
        },
        Body::Unreadable { .. } | Body::Listing(_) | Body::DeleteResult(_) => {
            stage.push(Finding::protocol(format!("answered {got} with a body")))
        }
    }
}

/// Where a document was called for and did not arrive: cut off, it could not be read; whole, it
/// is the wrong shape.
fn not_the_document(name: &str, body: &Body, stage: &mut Vec<Finding>) {
    stage.push(match body {
        Body::Cut { why, .. } => Finding::Unproved(format!("the {name} was cut off: {why}")),
        Body::Unreadable { why } => Finding::protocol(format!("the body is not a {name}: {why}")),
        _ => Finding::protocol(format!("the body is not a {name}")),
    });
}

/// The keys and sizes listed are exactly those written: one missing, extra, listed twice or of
/// another size is a disagreement with the model.
fn listing(response: &Response, expected: &[Listed], stage: &mut Vec<Finding>) {
    let Body::Listing(listed) = &response.body else {
        return not_the_document("listing", &response.body, stage);
    };
    let (mut got, mut want) = (listed.clone(), expected.to_vec());
    got.sort();
    want.sort();
    if got != want {
        stage.push(Finding::Fatal(
            FailureClass::Oracle,
            format!("listed {got:?}; wrote {want:?}"),
        ));
    }
}

/// No `<Error>` row (the gateway answers 200 even when keys failed,
/// `crates/gateway-s3/src/lib.rs:2396-2397`, `:2548`), and `<Deleted>` naming each key of `exact`
/// (sorted) once and nothing else. `exact` is `None` when the ask named a key twice, so no
/// document could match it.
fn delete_result(response: &Response, exact: Option<&[String]>, stage: &mut Vec<Finding>) {
    let Body::DeleteResult(result) = &response.body else {
        return not_the_document("<DeleteResult>", &response.body, stage);
    };
    if !result.errors.is_empty() {
        let rows: Vec<String> = result
            .errors
            .iter()
            .map(|failure| format!("{} ({})", failure.key, failure.code))
            .collect();
        stage.push(Finding::protocol(format!(
            "<Error> rows in the <DeleteResult>: {}",
            rows.join(", ")
        )));
    }
    if let Some(requested) = exact {
        let mut deleted = result.deleted.clone();
        deleted.sort();
        if deleted != requested {
            stage.push(Finding::protocol(format!(
                "<Deleted> names {deleted:?}; the delete requested {requested:?}"
            )));
        }
    }
}

/// Every follow-up is judged, in order, against the key it asked about, and everything it found
/// stays on the record: a follow-up of a requested key that did not prove the key gone says why
/// itself, whatever another follow-up of that key answered, so a clean 404 settles nothing but
/// its own probe. That leaves one way for a requested key to go unproved: nothing asked about it.
fn judge_follow_ups(
    requested: &[String],
    follow_ups: &[(String, Answer)],
    evidence: &mut Evidence,
) {
    for (key, answer) in follow_ups {
        let findings = follow_up(requested.contains(key), key, answer);
        evidence.follow_ups.extend(findings);
    }
    let probed = |key: &&String| follow_ups.iter().any(|(asked, _)| asked == *key);
    for key in requested.iter().filter(|key| !probed(key)) {
        evidence.follow_ups.push(Finding::Unproved(format!(
            "no follow-up HEAD proved {key:?} gone"
        )));
    }
}

/// One follow-up `HEAD`, status then body. Of a requested key: 404 is the defined answer, a 2xx
/// is `oracle`, anything else `protocol`. (The `oracle` holds "after an acknowledged delete": an
/// unacknowledged one is already fatal on its own, earlier, status.) Of any other key: the delete
/// names only its own keys and that key may well exist, so only the checks that do not depend on
/// the key run, and it can never prove anything.
fn follow_up(requested: bool, key: &str, answer: &Answer) -> Vec<Finding> {
    let response = match answer {
        Answer::Response(response) => response,
        Answer::NoAnswer { why } => {
            return vec![Finding::Unproved(format!(
                "the follow-up HEAD of {key:?} got no answer: {why}"
            ))];
        }
    };
    let got = response.status;
    let mut findings = Vec::new();
    if requested {
        if response.is_success() {
            findings.push(Finding::Fatal(
                FailureClass::Oracle,
                format!("{key:?} answered {got} to a HEAD after its delete"),
            ));
        } else if got != 404 {
            findings.push(Finding::protocol(format!(
                "the follow-up HEAD of {key:?} answered {got}; the row defines 404"
            )));
        }
    } else if (500..=599).contains(&got) {
        findings.push(Finding::protocol(format!(
            "the follow-up HEAD of {key:?} answered {got}, a 5xx"
        )));
    }
    no_body(response, &mut findings);
    if !requested {
        findings.push(Finding::Unproved(format!(
            "a follow-up HEAD of {key:?}, which the delete did not request"
        )));
    }
    findings
}

/// A strong entity-tag (RFC 9110 §8.8.3): `"` etagc* `"`, etagc = `%x21 / %x23-7E / obs-text`.
pub fn is_strong_etag(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2
        && bytes[0] == b'"'
        && bytes[bytes.len() - 1] == b'"'
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|&b| b == 0x21 || (0x23..=0x7E).contains(&b) || b >= 0x80)
}

/// `bytes first-last/size` (RFC 9110 §14.4): the unit case-insensitive (§14.1), each number one
/// or more ASCII digits (no `+` sign, which `u64::from_str` takes) that fits a `u64`.
fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (unit, rest) = value.split_once(' ')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (range, size) = rest.split_once('/')?;
    let (first, last) = range.split_once('-')?;
    // `parse` refuses an empty string and a number too big for a `u64`; the digit check refuses
    // the `+` it would take.
    let digits = |s: &str| {
        if s.bytes().all(|b| b.is_ascii_digit()) {
            s.parse::<u64>().ok()
        } else {
            None
        }
    };
    Some((digits(first)?, digits(last)?, digits(size)?))
}

// --- The run verdict ------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail,
    Inconclusive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fatal {
    pub operation: Operation,
    pub class: FailureClass,
    pub detail: String,
}

/// Why a row is not proved; `Undeclared`: exercised, but the profile declares no row for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unproved {
    NotExercised,
    NoAnswer { why: String },
    Unsettled { why: String },
    Undeclared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotCovered {
    pub operation: Operation,
    pub issue: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunVerdict {
    pub outcome: Outcome,
    /// Every `required` or `unsupported` row whose every exercise passed, in profile order.
    pub proved: Vec<Operation>,
    /// Every fatal result, in exercise order.
    pub fatal: Vec<Fatal>,
    /// Every row not proved, with the first reason found.
    pub unproved: Vec<(Operation, Unproved)>,
    /// Every `supported, not checked` row, exercised or not.
    pub not_covered: Vec<NotCovered>,
}

/// Any fatal result ⇒ FAIL, whatever else passed. Else INCONCLUSIVE when a `required` or
/// `unsupported` row was not exercised, or any exercise of it had no verdict or was unsettled
/// (a row is proved only when every exercise of it passed), or an exercise names an operation
/// the profile has no row for, or no row was proved (0017's vacuity gate). Else PASS. A
/// `supported, not checked` row is listed as not covered, and a run can PASS without it.
pub fn verdict(profile: &Profile, exercises: &[Exercise]) -> RunVerdict {
    let mut fatal = Vec::new();
    let mut unproved: Vec<(Operation, Unproved)> = Vec::new();
    let mut note = |operation: Operation, why: Unproved| {
        if !unproved.iter().any(|(op, _)| *op == operation) {
            unproved.push((operation, why));
        }
    };
    for exercise in exercises {
        let operation = exercise.operation();
        let Some(row) = profile.row(operation) else {
            note(operation, Unproved::Undeclared);
            continue;
        };
        match classify(row, exercise) {
            RowResult::Fatal { class, detail } => fatal.push(Fatal {
                operation,
                class,
                detail,
            }),
            // Neither proved nor needed: only a fatal answer to it counts.
            _ if !row.must_prove() => {}
            RowResult::Pass => {}
            RowResult::NoAnswer { why } => note(operation, Unproved::NoAnswer { why }),
            RowResult::Unsettled { why } => note(operation, Unproved::Unsettled { why }),
            // Only a `supported, not checked` row gives this; on any other it is still no pass.
            RowResult::NotChecked { issue } => note(
                operation,
                Unproved::Unsettled {
                    why: format!("not checked (#{issue})"),
                },
            ),
        }
    }
    let mut not_covered = Vec::new();
    for row in profile.rows() {
        match row.expected {
            Expected::SupportedNotChecked { issue } => not_covered.push(NotCovered {
                operation: row.operation,
                issue,
            }),
            Expected::Required | Expected::Unsupported { .. } => {
                if !exercises.iter().any(|e| e.operation() == row.operation) {
                    note(row.operation, Unproved::NotExercised);
                }
            }
        }
    }
    // A row that must be proved and is not noted unproved was exercised, and is proved unless
    // an exercise of it was fatal.
    let proved: Vec<Operation> = profile
        .rows()
        .iter()
        .filter(|row| row.must_prove())
        .map(|row| row.operation)
        .filter(|op| {
            !unproved.iter().any(|(o, _)| o == op) && !fatal.iter().any(|f| f.operation == *op)
        })
        .collect();
    let outcome = if !fatal.is_empty() {
        Outcome::Fail
    } else if !unproved.is_empty() || proved.is_empty() {
        Outcome::Inconclusive
    } else {
        Outcome::Pass
    };
    RunVerdict {
        outcome,
        proved,
        fatal,
        unproved,
        not_covered,
    }
}
