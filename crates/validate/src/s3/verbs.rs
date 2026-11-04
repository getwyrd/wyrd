//! The verbs `smoke` sends beyond PUT, GET and DELETE of one object (#938): HEAD, a ranged
//! GET, a conditional GET, both listings, bulk delete, and one probe of each operation the
//! capability matrix may declare unsupported ([`Probe`]).
//!
//! Every one reports what the server sent, read as `super::answer` describes: the head as it
//! arrived, and the body read by the client itself under [`super::BUFFERED_BODY_BUDGET`] and
//! the body deadlines, beside that head. None decides whether an answer is right; that is the
//! matrix's (743.1). A request the SDK never got an answer to is the [`S3Error`] the older
//! verbs report it as, through [`S3Client::classify`].

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::error::{BuildError, DisplayErrorContext, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use bytes::Bytes;

use super::answer::{
    answer, read_whole, settle, Answer, BodyFailure, Capture, Captured, Fetched, Head, NoResult,
    SuccessBody, Taken,
};
use super::document::{
    delete_rows, listing_page, DeleteResult, Listing, ListingError, Version, MAX_LISTING_PAGES,
};
use super::error::{BodyError, S3Error};
use super::{object_length, path_segment, ObjectBody, S3Client};

/// The `ETag` S3 gives an empty part, the part a [`Probe::UploadPart`] sends; the one part a
/// [`Probe::CompleteMultipartUpload`] names.
const EMPTY_PART_ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";

/// The longest URI the `http` crate takes (`http` 1.4.2 `src/uri/mod.rs:145`, `:296`).
const LONGEST_URI: usize = 65_534;

/// What a [`Probe`]'s key and upload id leave free of [`LONGEST_URI`], for the rest of the URI
/// the SDK writes around them. The most it writes is 40 bytes, [`Probe::UploadPart`]'s
/// (`/`, then `?x-id=UploadPart&partNumber=1&uploadId=`: `aws-sdk-s3` 1.155.0
/// `src/operation/upload_part.rs:403-427`).
const URI_ROOM: usize = 64;

/// One request for an operation the capability matrix may declare unsupported, sent as the
/// SDK sends that operation. Object and multipart probes name a key in the client's bucket;
/// the multipart ones after the first name an upload id the caller chooses, since none can be
/// created where `CreateMultipartUpload` is unsupported. [`Probe::CreateBucket`] and
/// [`Probe::DeleteBucket`] name their bucket. The client checks no bucket name against S3's
/// naming rules and protects no bucket: the name of a bucket the endpoint holds is sent as any
/// other, so what keeps a probe from creating or deleting a bucket its caller does not own is only the
/// name the caller gives (743.3 gives one those rules forbid). What the client gives the caller
/// is that the name reaches the endpoint as given: one request, to the
/// configured endpoint whether it is an IP address or a host name, at the endpoint's path,
/// `/`, then the name as one path segment, percent-encoded where a byte is not one of URI's
/// unreserved characters, signed for `s3` (`super::PathStyle`). That holds for every name,
/// those the SDK's own endpoint rules would treat otherwise included: one ending in `--x-s3`
/// or `--xa-s3`, one in the shape of an S3 on Outposts access-point alias, one starting with
/// `arn:`. A name of `.` or `..` is sent as that segment, which a server that resolves dot
/// segments reads as the endpoint's path itself or its parent. Two exceptions, each a probe
/// that sends nothing and is [`S3Error::RequestNotBuilt`]: a name that is empty or all white
/// space, which the SDK refuses as a missing bucket; and a name so long that the request's URL
/// would be longer than an `http` URI may be (65,534 bytes).
///
/// A key or an upload id is held to the same limit, more strictly. The SDK writes them into the
/// URI of the operation first, and panics when that URI is too long (`aws-sdk-s3` 1.155.0
/// `src/operation/create_multipart_upload.rs:370`), so [`S3Client::probe`] refuses first, as
/// [`S3Error::RequestNotBuilt`] with nothing sent, a probe whose key and upload id, percent-encoded
/// as the SDK encodes them, leave fewer than 64 of those bytes free. A key is counted with each
/// `/` as one byte, and every other byte but URI's unreserved characters as three. The source
/// key of a [`Probe::CopyObject`] is in a header, not the URI, and is not limited here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe {
    /// `PUT /{bucket}/{key}` with `x-amz-copy-source: {bucket}/{source_key}`, the source key
    /// as given (a key that needs percent-encoding must come encoded).
    CopyObject { key: String, source_key: String },
    /// `POST /{bucket}/{key}?uploads`.
    CreateMultipartUpload { key: String },
    /// `PUT /{bucket}/{key}?partNumber=1&uploadId={upload_id}`, with an empty part.
    UploadPart { key: String, upload_id: String },
    /// `POST /{bucket}/{key}?uploadId={upload_id}`, completing with the one empty part
    /// [`Probe::UploadPart`] sends.
    CompleteMultipartUpload { key: String, upload_id: String },
    /// `DELETE /{bucket}/{key}?uploadId={upload_id}`.
    AbortMultipartUpload { key: String, upload_id: String },
    /// `GET /{bucket}/{key}?uploadId={upload_id}`.
    ListParts { key: String, upload_id: String },
    /// `GET /{bucket}/?uploads`.
    ListMultipartUploads,
    /// `GET /{bucket}/?location`.
    GetBucketLocation,
    /// `PUT /{bucket}/`, with no `<CreateBucketConfiguration>`.
    CreateBucket { bucket: String },
    /// `DELETE /{bucket}/`.
    DeleteBucket { bucket: String },
    /// `GET /`.
    ListBuckets,
    /// `HEAD /{bucket}/`, the client's bucket.
    HeadBucket,
}

impl Probe {
    /// The S3 operation's name.
    pub fn operation(&self) -> &'static str {
        match self {
            Self::CopyObject { .. } => "CopyObject",
            Self::CreateMultipartUpload { .. } => "CreateMultipartUpload",
            Self::UploadPart { .. } => "UploadPart",
            Self::CompleteMultipartUpload { .. } => "CompleteMultipartUpload",
            Self::AbortMultipartUpload { .. } => "AbortMultipartUpload",
            Self::ListParts { .. } => "ListParts",
            Self::ListMultipartUploads => "ListMultipartUploads",
            Self::GetBucketLocation => "GetBucketLocation",
            Self::CreateBucket { .. } => "CreateBucket",
            Self::DeleteBucket { .. } => "DeleteBucket",
            Self::ListBuckets => "ListBuckets",
            Self::HeadBucket => "HeadBucket",
        }
    }

    /// Whether the request is a GET or a HEAD.
    fn reads(&self) -> bool {
        matches!(
            self,
            Self::ListParts { .. }
                | Self::ListMultipartUploads
                | Self::GetBucketLocation
                | Self::ListBuckets
                | Self::HeadBucket
        )
    }

    /// Why the SDK must not be handed this probe, when its key and upload id are too long for
    /// a URI ([`URI_ROOM`]).
    fn too_long(&self) -> Option<String> {
        let (key, upload_id) = match self {
            Self::CopyObject { key, .. } | Self::CreateMultipartUpload { key } => {
                (key.as_str(), "")
            }
            Self::UploadPart { key, upload_id }
            | Self::CompleteMultipartUpload { key, upload_id }
            | Self::AbortMultipartUpload { key, upload_id }
            | Self::ListParts { key, upload_id } => (key.as_str(), upload_id.as_str()),
            Self::ListMultipartUploads
            | Self::GetBucketLocation
            | Self::CreateBucket { .. }
            | Self::DeleteBucket { .. }
            | Self::ListBuckets
            | Self::HeadBucket => return None,
        };
        // The SDK encodes a key as a path, leaving its `/`s, and an upload id as a query value
        // (`aws-smithy-http` 0.64.3 `src/label.rs:14`, `src/query.rs:17`); its set is URI's
        // unreserved characters left as they are, `path_segment`'s (`src/urlencode.rs:9-42`).
        let encoded =
            path_segment(key).len() - 2 * key.matches('/').count() + path_segment(upload_id).len();
        (encoded + URI_ROOM > LONGEST_URI).then(|| {
            format!(
                "the key and upload id of this {} take {encoded} bytes percent-encoded, which \
                 leaves fewer than {URI_ROOM} of the {LONGEST_URI} bytes a URI may have",
                self.operation()
            )
        })
    }

    /// What the body of the operation's S3 success is: a document under the root the S3 API
    /// gives it, or none.
    fn success_body(&self) -> SuccessBody {
        match self {
            Self::CopyObject { .. } => SuccessBody::Document("CopyObjectResult"),
            Self::CreateMultipartUpload { .. } => {
                SuccessBody::Document("InitiateMultipartUploadResult")
            }
            Self::CompleteMultipartUpload { .. } => {
                SuccessBody::Document("CompleteMultipartUploadResult")
            }
            Self::ListParts { .. } => SuccessBody::Document("ListPartsResult"),
            Self::ListMultipartUploads => SuccessBody::Document("ListMultipartUploadsResult"),
            Self::GetBucketLocation => SuccessBody::Document("LocationConstraint"),
            Self::ListBuckets => SuccessBody::Document("ListAllMyBucketsResult"),
            Self::UploadPart { .. }
            | Self::AbortMultipartUpload { .. }
            | Self::CreateBucket { .. }
            | Self::DeleteBucket { .. }
            | Self::HeadBucket => SuccessBody::Empty,
        }
    }
}

/// The per-call config of a GET object verb, so that its body reaches the client as sent.
///
/// By default the SDK asks for a checksum (`x-amz-checksum-mode: ENABLED`, a header the caller
/// never asked for: `aws-sdk-s3` 1.155.0 `src/operation/get_object.rs:185`) and wraps the body
/// of any response that carries an `x-amz-checksum-*` header in a validator, at the same hook
/// [`Capture`] takes the body, and before it (`src/http_response_checksum.rs:130-139`, the
/// operation's plugin coming before the call's: `src/operation/get_object.rs:72-82`).
/// [`Capture`] would then read the validator: a `304` or an S3 error that carries such a header
/// would turn into a checksum mismatch, its code lost. `WhenRequired` turns off both.
/// `HeadObject`, the listings, bulk delete and the probes have no such validator.
fn body_as_sent() -> aws_sdk_s3::config::Builder {
    aws_sdk_s3::Config::builder()
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
}

impl S3Client {
    /// HEAD `key`. A `200` carries the object's size in its `Content-Length` and its `ETag`
    /// ([`Head::header`]); an error has no body, so its code is [`super::ErrorCode::NoBody`].
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
        Ok(self.answer(captured, SuccessBody::Empty).await)
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

    /// `DeleteObjects` of `keys` in one request, not quiet, so S3 answers with a row for each.
    /// S3 answers `200` whether or not every key was deleted: a key that was not is only an
    /// `<Error>` row ([`DeleteResult::errors`]).
    ///
    /// `Ok` says a whole `<DeleteResult>` was read, not that any key was deleted. The result is
    /// whatever rows the server sent, any number, for any keys; it names the requested keys no
    /// row names ([`DeleteResult::keys_without_a_row`]) and the rows that name no requested key
    /// ([`DeleteResult::rows_not_requested`]), which are the caller's to judge.
    ///
    /// The result is read whole under [`super::BUFFERED_BODY_BUDGET`] (64 KiB), which limits
    /// how much one request can ask, and the server has acted by the time the limit is found.
    /// The Wyrd gateway's result is 115 bytes, plus each row's key as XML escapes it, plus 30
    /// bytes of markup for a `<Deleted>` row. An `<Error>` row takes 113 bytes of markup, code
    /// and message for `InternalError`, 83 more than a `<Deleted>` row.
    ///
    /// So, with no failures, 1,000 keys of 32 bytes fit (62,115 bytes). 1,000 keys of 40 bytes do
    /// not: the result is 70,115 bytes, refused ([`super::BodyFailure::OverBudget`]) beside its
    /// `200`, never read in part, after the gateway has deleted the keys, so every row is lost,
    /// any `<Error>` row with them. Failures make the result larger, so those figures do not hold
    /// once keys fail: 1,000 keys of 32 bytes fit with 41 `InternalError` rows (65,518 bytes), and
    /// with 42 (65,601 bytes) are refused the same way, every row lost. A caller sizes its
    /// requests to fit with the failures it needs to read.
    pub async fn delete_objects<K: AsRef<str>>(
        &self,
        keys: &[K],
    ) -> Result<DeleteResult, NoResult> {
        let requested: Vec<String> = keys.iter().map(|key| key.as_ref().to_owned()).collect();
        let objects = requested
            .iter()
            .map(|key| ObjectIdentifier::builder().key(key).build())
            .collect::<Result<Vec<_>, _>>()
            .map_err(not_built)?;
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .build()
            .map_err(not_built)?;
        let (capture, taken) = Capture::new(false);
        let sent = self
            .sdk
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .customize()
            .interceptor(capture)
            .send()
            .await;
        let captured = self.captured(sent, &taken).map_err(NoResult::NoAnswer)?;
        let (head, body) = self.read(captured).await;
        match settle(&head, body, |body| delete_rows(body)) {
            Ok(rows) => Ok(DeleteResult {
                head,
                requested,
                rows,
            }),
            Err(outcome) => Err(NoResult::Answered(Answer { head, outcome })),
        }
    }

    /// Send `probe` and report its answer whole. A 2xx is [`super::Outcome::Success`] only
    /// when its body is the one the operation's S3 success carries; one whose body is an
    /// `<Error>` document instead is [`super::Outcome::Error`], its code and message beside the
    /// 2xx; one that stalls, is cut, or is neither keeps its status beside the failure.
    ///
    /// A probe whose key and upload id are too long for a URI is [`S3Error::RequestNotBuilt`],
    /// and nothing is sent ([`Probe`]).
    pub async fn probe(&self, probe: &Probe) -> Result<Answer, S3Error> {
        if let Some(detail) = probe.too_long() {
            return Err(S3Error::RequestNotBuilt { detail });
        }
        let (capture, taken) = Capture::new(probe.reads());
        let bucket = self.bucket.as_str();
        // Each arm builds one operation and sends it with the capture on.
        macro_rules! send {
            ($request:expr) => {
                self.captured(
                    $request.customize().interceptor(capture).send().await,
                    &taken,
                )
            };
        }
        let captured = match probe {
            Probe::CopyObject { key, source_key } => send!(self
                .sdk
                .copy_object()
                .bucket(bucket)
                .key(key)
                .copy_source(format!("{bucket}/{source_key}"))),
            Probe::CreateMultipartUpload { key } => {
                send!(self.sdk.create_multipart_upload().bucket(bucket).key(key))
            }
            Probe::UploadPart { key, upload_id } => send!(self
                .sdk
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(1)
                .content_length(0)
                .body(ByteStream::from_static(b""))),
            Probe::CompleteMultipartUpload { key, upload_id } => send!(self
                .sdk
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(
                            CompletedPart::builder()
                                .part_number(1)
                                .e_tag(EMPTY_PART_ETAG)
                                .build()
                        )
                        .build()
                )),
            Probe::AbortMultipartUpload { key, upload_id } => send!(self
                .sdk
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)),
            Probe::ListParts { key, upload_id } => send!(self
                .sdk
                .list_parts()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)),
            Probe::ListMultipartUploads => {
                send!(self.sdk.list_multipart_uploads().bucket(bucket))
            }
            Probe::GetBucketLocation => send!(self.sdk.get_bucket_location().bucket(bucket)),
            Probe::CreateBucket { bucket } => send!(self.sdk.create_bucket().bucket(bucket)),
            Probe::DeleteBucket { bucket } => send!(self.sdk.delete_bucket().bucket(bucket)),
            Probe::ListBuckets => send!(self.sdk.list_buckets()),
            Probe::HeadBucket => send!(self.sdk.head_bucket().bucket(bucket)),
        }?;
        Ok(self.answer(captured, probe.success_body()).await)
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
                // endpoint `super::PathStyle` refuses, a URL too long to apply) as a dispatch
                // failure, which reads as no response; but nothing was sent. Nothing at all:
                // before that hook this client only resolves the endpoint and signs with
                // static credentials, and never sends an S3 Express `CreateSession` of the
                // SDK's own (`S3Client::with_deadlines`).
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

    /// `captured` as a whole answer, a 2xx read as `success` describes.
    async fn answer(&self, captured: Captured, success: SuccessBody) -> Answer {
        let (head, body) = self.read(captured).await;
        answer(head, body, success)
    }

    /// `captured`, answering a GET: a 2xx streams its body, anything else is read whole.
    async fn fetched(&self, captured: Captured) -> Fetched {
        if !captured.head.is_success() {
            return Fetched::Other(self.answer(captured, SuccessBody::Empty).await);
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

/// A request the SDK's builders refused, before anything was sent.
fn not_built(e: BuildError) -> NoResult {
    NoResult::NoAnswer(S3Error::RequestNotBuilt {
        detail: DisplayErrorContext(&e).to_string(),
    })
}
