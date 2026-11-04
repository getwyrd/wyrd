//! The documents the client reads from a 2xx itself (#938): a listing page, `ListObjectsV2`'s
//! or `ListObjects`'s `<ListBucketResult>`, and a bulk delete's `<DeleteResult>`.
//!
//! Each is read by the rules every document the client reads is (UTF-8, one well-formed
//! document under the expected root and nothing around it but whitespace and comments,
//! bounded depth: `super::response::whole_document`), from a body read under
//! [`super::BUFFERED_BODY_BUDGET`]. The fields read are text only and given at most once; a
//! field given twice, holding markup, or missing where S3 always sends it is a refusal, never
//! a guess. Elements a listing page or a row holds that the client does not read are passed
//! over; a `<DeleteResult>`'s own children are not, since a row left unread could be a failed
//! key (S3 sends none there but `<Deleted>` and `<Error>`). Nothing here judges what the server
//! answered (which keys, how many, whether a failure was due): that is the caller's.

use std::collections::HashSet;
use std::fmt;

use super::answer::{Head, NoResult};
use super::error::ErrorCode;
use super::response::{named_code, text_only, whole_document};

/// The most pages one listing call reads. A listing still truncated after them is
/// [`ListingError::TooManyPages`], so an endpoint that answers every page with another
/// continuation cannot hold the call, or grow what it holds, without end: at most this many
/// pages, each under [`super::BUFFERED_BODY_BUDGET`].
pub const MAX_LISTING_PAGES: usize = 100;

/// A whole listing: every page read, from the first to the one that ended it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    /// Every `<Contents>` of every page, in the order the pages listed them.
    pub entries: Vec<ListEntry>,
    /// Each page's head, in order: as many as requests were sent.
    pub pages: Vec<Head>,
}

/// One `<Contents>` of a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListEntry {
    /// The `<Key>`, as sent.
    pub key: String,
    /// The `<Size>`, a decimal number of bytes.
    pub size: u64,
    /// The `<ETag>`, as sent (quotes included), when the entry has one.
    pub etag: Option<String>,
}

/// Why a listing call has no listing. No entry of an earlier page is reported: a listing is
/// reported whole or not at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListingError {
    /// Page `page` (the first is 1) holds no listing page: no answer, an answer other than a
    /// 2xx, or a 2xx whose body failed or is not one whole listing page the client can
    /// continue from.
    Page { page: usize, cause: NoResult },
    /// The listing was still truncated after `pages` pages ([`MAX_LISTING_PAGES`]).
    TooManyPages { pages: usize },
}

impl fmt::Display for ListingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Page { page, cause } => write!(f, "listing page {page}: {cause}"),
            Self::TooManyPages { pages } => {
                write!(f, "the listing was still truncated after {pages} pages")
            }
        }
    }
}

impl std::error::Error for ListingError {}

/// A whole `<DeleteResult>`: the answer's head, the keys the request named, and every row in
/// document order.
///
/// A row's key is compared with the requested keys as text, once the document's XML escapes are
/// read and its line endings normalised as XML requires (XML 1.0 §2.11, which `roxmltree`
/// applies to every text node): a raw carriage return, alone or before a line feed, is read as
/// one line feed, while one escaped as `&#13;` is read as the carriage return. So a key that
/// holds a carriage return and is written into the document raw, as the Wyrd gateway writes
/// it, comes back with a line feed in its place: that row names no requested key, and the
/// requested key has no row. Case and white space are compared as they are. What the comparison
/// finds is reported and never judged: a result that accounts for none of the requested keys is
/// still a result, and whether it is a failure is the caller's to say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteResult {
    /// The head of the answer that carried it.
    pub head: Head,
    /// The keys the request named, in the order given.
    pub requested: Vec<String>,
    /// Every `<Deleted>` and `<Error>` row, in document order.
    pub rows: Vec<DeleteRow>,
}

impl DeleteResult {
    /// The key of every `<Deleted>` row, in document order.
    pub fn deleted(&self) -> impl Iterator<Item = &str> {
        self.rows.iter().filter_map(|row| match row {
            DeleteRow::Deleted { key } => Some(key.as_str()),
            DeleteRow::Error(_) => None,
        })
    }

    /// Every `<Error>` row, in document order.
    pub fn errors(&self) -> impl Iterator<Item = &KeyError> {
        self.rows.iter().filter_map(|row| match row {
            DeleteRow::Error(error) => Some(error),
            DeleteRow::Deleted { .. } => None,
        })
    }

    /// Every requested key no row names, in the order given (a key given twice, twice).
    pub fn keys_without_a_row(&self) -> impl Iterator<Item = &str> {
        let named: HashSet<&str> = self.rows.iter().map(DeleteRow::key).collect();
        self.requested
            .iter()
            .map(String::as_str)
            .filter(move |key| !named.contains(key))
    }

    /// Every row that names no requested key, in document order.
    pub fn rows_not_requested(&self) -> impl Iterator<Item = &DeleteRow> {
        let requested: HashSet<&str> = self.requested.iter().map(String::as_str).collect();
        self.rows
            .iter()
            .filter(move |row| !requested.contains(row.key()))
    }
}

/// One row of a `<DeleteResult>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteRow {
    /// A `<Deleted>` row: the key the server reports deleted.
    Deleted { key: String },
    /// An `<Error>` row: a key the server reports it did not delete.
    Error(KeyError),
}

impl DeleteRow {
    /// The key the row names.
    pub fn key(&self) -> &str {
        match self {
            Self::Deleted { key } => key,
            Self::Error(error) => &error.key,
        }
    }
}

/// An `<Error>` row of a `<DeleteResult>`, as sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyError {
    /// The row's `<Key>`.
    pub key: String,
    /// The row's `<Code>`, or [`ErrorCode::MissingInXml`] when it has none or an empty one
    /// (never [`ErrorCode::NoBody`]).
    pub code: ErrorCode,
    /// The row's `<Message>`, when it has one.
    pub message: Option<String>,
}

/// Which listing call a page answers: they continue differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Version {
    /// `ListObjectsV2`: from the page's `<NextContinuationToken>`.
    V2,
    /// `ListObjects`: from the page's `<NextMarker>`, or, when it gives none (S3 gives one only
    /// with a delimiter), after the page's last key.
    V1,
}

/// One page of a listing, read.
#[derive(Debug)]
pub(super) struct Page {
    pub(super) entries: Vec<ListEntry>,
    /// What the next request continues from, or `None` when this page ends the listing.
    pub(super) next: Option<String>,
}

const LISTING: &str = "ListBucketResult";
const DELETE_RESULT: &str = "DeleteResult";

/// A `<ListBucketResult>` page for `version`. `<IsTruncated>` is required and is exactly
/// `true` or `false`; every `<Contents>` has exactly one `<Key>` and one `<Size>` (decimal
/// digits only) and at most one `<ETag>`. A truncated page must say what the next request
/// continues from (see [`Version`]); a page that cannot is refused, since following it would
/// either stop short or start over.
pub(super) fn listing_page(body: &[u8], version: Version) -> Result<Page, String> {
    let document = whole_document(body, LISTING)
        .map_err(|fault| fault.describe("the listing body", LISTING))?;
    let root = document.root_element();
    let [truncated, token, marker] = fields(
        root,
        LISTING,
        ["IsTruncated", "NextContinuationToken", "NextMarker"],
    )?;
    let truncated = match truncated.as_deref() {
        Some("true") => true,
        Some("false") => false,
        Some(other) => {
            return Err(format!(
                "the listing's <IsTruncated> is {other:?}, not \"true\" or \"false\""
            ))
        }
        None => return Err("the listing names no <IsTruncated>".to_string()),
    };
    let mut entries = Vec::new();
    for contents in root
        .children()
        .filter(|node| node.is_element() && node.tag_name().name() == "Contents")
    {
        let [key, size, etag] = fields(contents, "Contents", ["Key", "Size", "ETag"])?;
        let key = key.ok_or("a <Contents> of the listing names no <Key>")?;
        let size = size.ok_or_else(|| format!("the <Contents> of {key:?} names no <Size>"))?;
        let size = decimal(&size).ok_or_else(|| {
            format!("the <Size> of {key:?} is {size:?}, not a decimal number of bytes")
        })?;
        entries.push(ListEntry { key, size, etag });
    }
    let next = match (truncated, version) {
        (false, _) => None,
        (true, Version::V2) => Some(token.filter(|token| !token.is_empty()).ok_or(
            "the listing page is truncated and names no <NextContinuationToken> to continue from",
        )?),
        (true, Version::V1) => Some(
            marker
                .filter(|marker| !marker.is_empty())
                .or_else(|| entries.last().map(|entry| entry.key.clone()))
                .filter(|from| !from.is_empty())
                .ok_or(
                    "the listing page is truncated, names no <NextMarker> and lists no key to \
                     continue after",
                )?,
        ),
    };
    Ok(Page { entries, next })
}

/// The rows of a `<DeleteResult>`, in document order: every `<Deleted>` with exactly one
/// `<Key>`, every `<Error>` with exactly one `<Key>` and at most one `<Code>` and one
/// `<Message>`. An empty `<Key>` names no key, as an empty `<Code>` names no code; one of white
/// space does, since S3 allows such a key. Anything else the root holds but whitespace and
/// comments is refused, a processing instruction included: the rows are the only evidence of a
/// failed key, so none goes unread.
pub(super) fn delete_rows(body: &[u8]) -> Result<Vec<DeleteRow>, String> {
    let document = whole_document(body, DELETE_RESULT)
        .map_err(|fault| fault.describe("the delete result body", DELETE_RESULT))?;
    let mut rows = Vec::new();
    for child in document.root_element().children() {
        if child.is_text() {
            if !child.text().is_some_and(is_xml_whitespace) {
                return Err("the <DeleteResult> holds text outside any row".to_string());
            }
            continue;
        }
        if child.is_comment() {
            continue;
        }
        if child.is_pi() {
            return Err("the <DeleteResult> holds a processing instruction".to_string());
        }
        match child.tag_name().name() {
            "Deleted" => {
                let [key] = fields(child, "Deleted", ["Key"])?;
                let key = named_key(key).ok_or("a <Deleted> row names no <Key>")?;
                rows.push(DeleteRow::Deleted { key });
            }
            "Error" => {
                let [key, code, message] = fields(child, "Error", ["Key", "Code", "Message"])?;
                let key = named_key(key).ok_or("an <Error> row names no <Key>")?;
                rows.push(DeleteRow::Error(KeyError {
                    key,
                    code: named_code(code),
                    message,
                }));
            }
            other => {
                return Err(format!(
                    "the <DeleteResult> holds a <{other}>, neither a <Deleted> nor an <Error> row"
                ))
            }
        }
    }
    Ok(rows)
}

/// `key`, unless it is absent or empty.
fn named_key(key: Option<String>) -> Option<String> {
    key.filter(|key| !key.is_empty())
}

/// Whether `text` is XML white space and nothing else.
fn is_xml_whitespace(text: &str) -> bool {
    text.bytes()
        .all(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
}

/// The text of `parent`'s child elements named `names`, each at most once and holding text
/// only; children of other names are passed over (a row's `<VersionId>`, say). `parent_name` names `parent` in a refusal.
fn fields<const N: usize>(
    parent: roxmltree::Node<'_, '_>,
    parent_name: &str,
    names: [&str; N],
) -> Result<[Option<String>; N], String> {
    let mut found: [Option<String>; N] = std::array::from_fn(|_| None);
    for child in parent.children().filter(roxmltree::Node::is_element) {
        let name = child.tag_name().name();
        let Some(slot) = names.iter().position(|&wanted| wanted == name) else {
            continue;
        };
        let text = text_only(child).ok_or_else(|| {
            format!("the <{name}> of a <{parent_name}> holds markup, not only text")
        })?;
        if found[slot].replace(text).is_some() {
            return Err(format!("a <{parent_name}> has more than one <{name}>"));
        }
    }
    Ok(found)
}

/// `text` as a decimal number: ASCII digits only (no sign, no space, not empty), at most
/// `u64::MAX`.
fn decimal(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}
