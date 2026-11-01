//! The verbs `smoke` sends beyond PUT, GET and DELETE of one object that only read (#938):
//! HEAD, a ranged GET, a conditional GET, and both listings.
//!
//! Every one reports what the server sent, read as `super::answer` describes: the head as it
//! arrived, and the body read by the client itself under [`super::BUFFERED_BODY_BUDGET`] and
//! the body deadlines, beside that head. None decides whether an answer is right; that is the
//! matrix's (743.1). A request the SDK never got an answer to is the [`S3Error`] the older
//! verbs report it as, through [`S3Client::classify`].

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;

use super::answer::{
    answer, read_whole, settle, Answer, BodyFailure, Capture, Captured, Fetched, Head, NoResult,
    Taken,
};
use super::document::{listing_page, Listing, ListingError, Version, MAX_LISTING_PAGES};
use super::error::{BodyError, S3Error};
use super::{object_length, ObjectBody, S3Client};

/// The per-call config of a GET object verb, so that its body reaches the client as sent.
///
/// By default the SDK asks for a checksum (`x-amz-checksum-mode: ENABLED`, a header the caller
/// never asked for: `aws-sdk-s3` 1.155.0 `src/operation/get_object.rs:185`) and wraps the body
/// of any response that carries an `x-amz-checksum-*` header in a validator, at the same hook
/// [`Capture`] takes the body, and before it (`src/http_response_checksum.rs:130-139`, the
/// operation's plugin coming before the call's: `src/operation/get_object.rs:72-82`).
/// [`Capture`] would then read the validator: a `304` or an S3 error that carries such a header
/// would turn into a checksum mismatch, its code lost. `WhenRequired` turns off both.
/// `HeadObject` and the listings have no such validator.
fn body_as_sent() -> aws_sdk_s3::config::Builder {
    aws_sdk_s3::Config::builder()
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
}

impl S3Client {
    /// HEAD `key`. A `200` carries the object's size in its `Content-Length` and its `ETag`
    /// ([`Head::header`]); an error has no body, so its code is [`super::ErrorCode::NoBody`].
    // deferred: #956 — these verbs reach the living architecture doc with the rest of this
    // client's surface there; the split of #938 gives that doc to #956.
    pub async fn head_object(&self, key: &str) -> Result<Answer, S3Error> {
        let (capture, taken) = Capture::new(true);
        let sent = self
            .sdk
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .customize()
            .interceptor(capture)
            .send()
            .await;
        let captured = self.captured(sent, &taken)?;
        Ok(self.answer(captured).await)
    }

    /// GET bytes `first` to `last` (inclusive) of `key`: `Range: bytes={first}-{last}`, as
    /// given. A `206` streams exactly the bytes its `Content-Length` declares; its
    /// `Content-Range` is in the head as sent.
    pub async fn get_object_range(
        &self,
        key: &str,
        first: u64,
        last: u64,
    ) -> Result<Fetched, S3Error> {
        let (capture, taken) = Capture::new(true);
        let sent = self
            .sdk
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(format!("bytes={first}-{last}"))
            .customize()
            .config_override(body_as_sent())
            .interceptor(capture)
            .send()
            .await;
        let captured = self.captured(sent, &taken)?;
        Ok(self.fetched(captured).await)
    }

    /// GET `key` with `If-None-Match: {etag}`, the tag as given. A `304` is
    /// [`Fetched::Other`], with no body; a `200` streams the whole object.
    pub async fn get_object_if_none_match(
        &self,
        key: &str,
        etag: &str,
    ) -> Result<Fetched, S3Error> {
        let (capture, taken) = Capture::new(true);
        let sent = self
            .sdk
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match(etag)
            .customize()
            .config_override(body_as_sent())
            .interceptor(capture)
            .send()
            .await;
        let captured = self.captured(sent, &taken)?;
        Ok(self.fetched(captured).await)
    }

    /// `ListObjectsV2` of the keys under `prefix`, `page_size` keys a page (`max-keys`), every
    /// page followed from its `<NextContinuationToken>` to the one that is not truncated.
    ///
    /// Each page is read whole under [`super::BUFFERED_BODY_BUDGET`], so `page_size` must leave
    /// a page room to fit: S3 spends a few hundred bytes of markup on an entry besides its key,
    /// so a page holds a couple of hundred entries with short keys, and far fewer with keys
    /// near S3's 1024-byte limit. A page past the budget is refused
    /// ([`super::BodyFailure::OverBudget`]), never read in part.
    pub async fn list_objects_v2(
        &self,
        prefix: &str,
        page_size: u16,
    ) -> Result<Listing, ListingError> {
        let mut listing = Listing::default();
        let mut token = None;
        for page in 1..=MAX_LISTING_PAGES {
            let (capture, taken) = Capture::new(true);
            let sent = self
                .sdk
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .max_keys(i32::from(page_size))
                .set_continuation_token(token.take())
                .customize()
                .interceptor(capture)
                .send()
                .await;
            match self.page(sent, &taken, Version::V2, &mut listing).await {
                Ok(None) => return Ok(listing),
                Ok(next) => token = next,
                Err(cause) => return Err(ListingError::Page { page, cause }),
            }
        }
        Err(ListingError::TooManyPages {
            pages: MAX_LISTING_PAGES,
        })
    }

    /// `ListObjects` (version 1) of the keys under `prefix`, `page_size` keys a page
    /// (`max-keys`), every page followed with a `marker` to the one that is not truncated. Each
    /// page is read under the budget as [`S3Client::list_objects_v2`]'s is.
    pub async fn list_objects_v1(
        &self,
        prefix: &str,
        page_size: u16,
    ) -> Result<Listing, ListingError> {
        let mut listing = Listing::default();
        let mut marker = None;
        for page in 1..=MAX_LISTING_PAGES {
            let (capture, taken) = Capture::new(true);
            let sent = self
                .sdk
                .list_objects()
                .bucket(&self.bucket)
                .prefix(prefix)
                .max_keys(i32::from(page_size))
                .set_marker(marker.take())
                .customize()
                .interceptor(capture)
                .send()
                .await;
            match self.page(sent, &taken, Version::V1, &mut listing).await {
                Ok(None) => return Ok(listing),
                Ok(next) => marker = next,
                Err(cause) => return Err(ListingError::Page { page, cause }),
            }
        }
        Err(ListingError::TooManyPages {
            pages: MAX_LISTING_PAGES,
        })
    }

    /// What the capture of one call took, or, when no response reached it, the SDK's failure
    /// classified as the older verbs classify it.
    fn captured<O, E>(
        &self,
        sent: Result<O, SdkError<E, HttpResponse>>,
        taken: &Taken,
    ) -> Result<Captured, S3Error>
    where
        E: std::error::Error + 'static,
    {
        let sdk_detail = || match &sent {
            Err(e) => format!(
                "the SDK stopped before the client could take the body: {}",
                DisplayErrorContext(e)
            ),
            Ok(_) => "the SDK finished the call without handing the body over".to_string(),
        };
        if let Some(captured) = taken.take(sdk_detail) {
            return Ok(captured);
        }
        match sent {
            Err(e) => Err(match self.classify(e) {
                // The SDK reports a request that failed before it called the connector (an
                // endpoint or a bucket its endpoint rules refuse, a URL too long to apply) as
                // a dispatch failure, which reads as no response; but the request asked for
                // was never sent.
                // deferred: #956 — one request of the SDK's own can come before that hook:
                // for a bucket its endpoint rules read as an S3 Express one, identity
                // resolution (`aws-smithy-runtime` 1.16.2 `client/orchestrator.rs:486`, before
                // `read_before_transmit` at `:514`) sends a `CreateSession` first (`aws-sdk-s3`
                // 1.155.0 `s3_express.rs:471`). #956 resolves endpoints itself and turns S3
                // Express session auth off.
                S3Error::NoResponse { detail } if !taken.transmitted() => {
                    S3Error::RequestNotBuilt { detail }
                }
                classified => classified,
            }),
            // An output is deserialized from a response, and the SDK runs `read_after_transmit`
            // on every response before it deserializes it (`aws-smithy-runtime` 1.16.0
            // `client/orchestrator.rs:507-514`).
            Ok(_) => unreachable!("the capture records every response the SDK deserializes"),
        }
    }

    /// The head of `captured`, and its body read whole.
    async fn read(&self, captured: Captured) -> (Head, Result<Bytes, BodyFailure>) {
        let Captured {
            head,
            framing,
            body,
        } = captured;
        let body = match body {
            Ok(body) => {
                read_whole(
                    body,
                    head.status,
                    framing,
                    self.deadlines.body_idle,
                    self.deadlines.body,
                )
                .await
            }
            Err(detail) => Err(BodyFailure::Transport {
                received: 0,
                detail,
            }),
        };
        (head, body)
    }

    /// `captured` as a whole answer.
    async fn answer(&self, captured: Captured) -> Answer {
        let (head, body) = self.read(captured).await;
        answer(head, body)
    }

    /// `captured`, answering a GET: a 2xx streams its body, anything else is read whole.
    async fn fetched(&self, captured: Captured) -> Fetched {
        if !captured.head.is_success() {
            return Fetched::Other(self.answer(captured).await);
        }
        let Captured {
            head,
            framing,
            body,
        } = captured;
        let body = body
            .map_err(|detail| BodyError::Transport {
                declared: None,
                received: 0,
                detail,
            })
            .and_then(|body| {
                let declared = object_length(framing)?;
                // The whole-body deadline starts here, as the response head has just been read.
                Ok(Box::new(ObjectBody::new(
                    ByteStream::new(body),
                    declared,
                    self.deadlines.body_idle,
                    self.deadlines.body,
                )))
            });
        Fetched::Object { head, body }
    }

    /// One listing page: its entries added to `listing` and what the next page continues from,
    /// or why it holds no page. `listing` is untouched by a page that holds none.
    async fn page<O, E>(
        &self,
        sent: Result<O, SdkError<E, HttpResponse>>,
        taken: &Taken,
        version: Version,
        listing: &mut Listing,
    ) -> Result<Option<String>, NoResult>
    where
        E: std::error::Error + 'static,
    {
        let captured = self.captured(sent, taken).map_err(NoResult::NoAnswer)?;
        let (head, body) = self.read(captured).await;
        match settle(&head, body, |body| listing_page(body, version)) {
            Ok(page) => {
                listing.entries.extend(page.entries);
                listing.pages.push(head);
                Ok(page.next)
            }
            Err(outcome) => Err(NoResult::Answered(Answer { head, outcome })),
        }
    }
}
