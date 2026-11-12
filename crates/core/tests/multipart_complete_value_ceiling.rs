//! Issue #1011 — `CompleteMultipartUpload` sizes the part sets its flip installs against the value
//! ceiling, `wyrd_core::multipart::complete::complete_upload`, driven over a real store
//! (`RedbMetadataStore::in_memory()`) behind a wrapper that refuses, as FoundationDB does
//! (`2103 value_too_large`), any batch putting a value past [`MAX_VALUE_BYTES`].
//!
//! The flip installs each part set inside one obligation value: the published parts'
//! `retire:records:{parts}` and the unnamed parts' `retire:bytes:{parts}` (`0016:662-663`). A set
//! is range-encoded, one run per isolated part number, and the format allows numbers up to
//! 999,999, so a sparse set can encode past the ceiling (#842 against `0016:382-384`). Such a flip
//! can never land; the request must be refused `SessionTooLarge` before it writes anything.
//!
//! Every session here is made by Create (`create_session`, #894). Parts are planted as canonical
//! `part:` / `psum:` records, as the part commit writes them, with no staging.
//!
//! * **(I1)** Parts 1 to 10,000, a Complete naming the odd ones: it publishes, and both `{parts}`
//!   obligations fit (58,901 and 58,909 bytes), the widest sets inside S3's part-number range.
//! * **(I2)** Parts 1, 3, …, 19,999, a Complete naming all of them (128,901 bytes as a `{parts}`
//!   value): refused `SessionTooLarge` with zero commits; the session stays `Open` with
//!   `attempts` unchanged, and an Abort then fences it.
//! * **(I3)** The boundary is the stored value, not the bare set: parts 1, 3, …, 15,869 encode to
//!   99,981 bytes and 99,991 as the value, and publish; parts 1, 3, …, 15,871 encode to 99,995
//!   bytes, inside the ceiling, but 100,005 as the value, and are refused with zero commits. A set
//!   whose value is exactly 100,000 bytes, the ceiling itself, publishes.
//! * **(I4)** A part committed after the first read: the set fits when the Complete first reads it,
//!   and the wrapper plants one more unnamed part just before the fence commits, which carries the
//!   unnamed parts' `retire:bytes:` value across the ceiling. The Complete fences, refuses against
//!   the frozen set, and releases: `Open@2`, `attempts` given back. Without the plant the same
//!   request publishes. With that part there from the start, the unnamed set is past the ceiling
//!   at the first read, and the request is refused with zero commits: the check before the fence
//!   measures the unnamed parts' obligation as well as the published parts'.
//!
//! The sizes are literals here, and every stored obligation is compared with a spelling of the
//! range encoding written in this file, so a change to either has to change this test too.
//!
//! The wrapper keeps its state in struct fields, never in a `static` (ADR-0035).

#![forbid(unsafe_code)]

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self, InodeId, SegmentNonce, MAX_VALUE_BYTES};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::complete::{
    complete_upload, CompleteError, CompleteRequest, NamedParts, PublishRequest,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    decode_part_record, decode_part_summary, decode_retire_obligation, decode_session_record,
    mpu_key, part_key, psum_key, retire_key, AbortOutcome, Budget, CompleteOutcome, CreateOutcome,
    Digest, PartNumber, PartNumberSet, PartScope, PartSummary, Refusal, RetireMode, RetireToken,
    SessionState, UploadId,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{
    BoxError, CommitOutcome, MetadataStore, Result as StoreResult, ScanPage, WriteBatch,
};

// ─────────────────────────────── fixtures ───────────────────────────────

const BUCKET: &str = "photos";
/// The root inode every Alpha object dirent lives under, as `<bucket>/<key>` (ADR-0046).
const ROOT: InodeId = 0;
const CLOCK: &str = "wall";
const CREATED_AT: u64 = 1_000;
const COMMITTED_AT: u64 = 1_500;
const FENCED_AT: u64 = 2_000;
const COMPLETED_AT: u64 = 3_000;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
/// The inode a flip that finds the target name free creates.
const FRESH_INODE: InodeId = 42;
/// Every planted part is one chunk of this many bytes.
const CHUNK_LEN: u64 = 1_024;
/// Where the planted chunk ids start.
const CHUNK_BASE: u64 = 1_000_000;
/// Parts planted per fixture batch: two puts each, well inside one batch's envelope.
const PLANT_BATCH: usize = 200;
/// No leg may hang the suite.
const LEG_TIMEOUT: Duration = Duration::from_secs(300);
/// The epoch a fence from `Open@0` puts the session at: every obligation the flip installs is
/// under it.
const FENCE_EPOCH: u64 = 1;

/// The stated sizes, in bytes, of the `{"parts":…}` values the legs build, and of two bare sets.
/// Written out rather than computed, so a change to the encoding has to change them here.
const ODD_TO_9_999_VALUE: usize = 58_901;
const EVEN_TO_10_000_VALUE: usize = 58_909;
const ODD_TO_19_999_VALUE: usize = 128_901;
const ODD_TO_15_869_SET: usize = 99_981;
const ODD_TO_15_869_VALUE: usize = 99_991;
const ODD_TO_15_871_SET: usize = 99_995;
const ODD_TO_15_871_VALUE: usize = 100_005;
const AT_CEILING_VALUE: usize = 100_000;

// (I3)'s pairs straddle the ceiling where the legs say: inside, both the bare set and the value
// fit; past, the bare set fits and the value does not; at, the value is the ceiling itself.
const _: () = assert!(ODD_TO_15_869_SET < ODD_TO_15_869_VALUE);
const _: () = assert!(ODD_TO_15_869_VALUE <= MAX_VALUE_BYTES);
const _: () = assert!(ODD_TO_15_871_SET <= MAX_VALUE_BYTES);
const _: () = assert!(ODD_TO_15_871_VALUE > MAX_VALUE_BYTES);
const _: () = assert!(AT_CEILING_VALUE == MAX_VALUE_BYTES);

async fn within<T>(leg: impl Future<Output = T>) -> T {
    tokio::time::timeout(LEG_TIMEOUT, leg)
        .await
        .expect("the leg settled in time")
}

/// A profile admitting `n` sessions: every per-session cap at 1, so `U_ref` is 2. Complete reads
/// no budget; the parts are planted, not admitted.
fn profile_of(n: u64) -> Budget {
    let json = format!(
        "{{\"w_ref\":{},\"max_part_chunks\":1,\"max_parts_per_session\":1,\
         \"max_inflight_parts\":1,\"max_staged_chunks\":1}}",
        2 * n
    );
    let profile: Budget = metadata::decode(json.as_bytes()).expect("a legal profile");
    assert_eq!(profile.max_sessions(), n);
    profile
}

/// The runtime Create needs: one fixed identity, no jitter, no wait.
struct Fixed(Option<(UploadId, SegmentNonce)>);

impl CreateRuntime for Fixed {
    fn mint(&mut self) -> (UploadId, SegmentNonce) {
        self.0.take().expect("one identity per create")
    }

    fn jitter(&mut self) -> u64 {
        0
    }

    fn wait(&mut self, _millis: u64) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
}

/// Part `n`'s digest: its number in the first four bytes, so every part's differs.
fn digest_of(n: u32) -> Digest {
    let mut bytes = [0x5a; 32];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    Digest::from_bytes(bytes)
}

/// The odd numbers from 1 to `last`.
fn odd_to(last: u32) -> Vec<u32> {
    (1..=last).step_by(2).collect()
}

/// The even numbers from 2 to `last`.
fn even_to(last: u32) -> Vec<u32> {
    (2..=last).step_by(2).collect()
}

/// The list naming `numbers`, each with its part's digest.
fn named(numbers: &[u32]) -> NamedParts {
    NamedParts::new(numbers.iter().map(|&n| (part(n), digest_of(n))).collect()).unwrap()
}

/// The blind batch that plants part `n`: its `part:` record naming one chunk and its `psum:`
/// summary, together, as the part commit writes them (`0016:660`), each in the codec's own
/// canonical spelling — checked by decoding it and re-encoding it.
fn plant_part(id: &UploadId, n: u32) -> WriteBatch {
    let digest = digest_of(n);
    let record = format!(
        "{{\"chunks\":[{{\"id\":{},\"scheme\":\"None\",\"len\":{CHUNK_LEN},\"placement\":[0]}}],\
         \"len\":{CHUNK_LEN},\"digest\":\"{digest}\",\"committed_at_millis\":{COMMITTED_AT},\
         \"session_epoch\":0}}",
        CHUNK_BASE + u64::from(n),
    );
    let decoded = decode_part_record(record.as_bytes()).expect("a canonical part");
    assert_eq!(metadata::encode(&decoded), record.as_bytes());
    let summary = PartSummary {
        chunks: 1,
        len: CHUNK_LEN,
        digest,
        committed_at_millis: COMMITTED_AT,
    };
    let psum = metadata::encode(&summary);
    assert_eq!(decode_part_summary(&psum).unwrap(), summary);
    WriteBatch::new()
        .put(part_key(id, part(n)), Bytes::from(record))
        .put(psum_key(id, part(n)), psum)
}

/// The `{"parts":…}` value naming `numbers`, spelled here rather than by the codec
/// ([`bare_set`]).
fn parts_value(numbers: &[u32]) -> String {
    format!("{{\"parts\":{}}}", bare_set(numbers))
}

/// The range encoding of `numbers` alone (`0016:382-388`), spelled here: each maximal run of
/// consecutive numbers `[lo,hi]`, an isolated number `[n,n]`.
fn bare_set(numbers: &[u32]) -> String {
    assert!(
        numbers.windows(2).all(|pair| pair[1] > pair[0]),
        "strictly ascending part numbers"
    );
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for &n in numbers {
        match runs.last_mut() {
            Some(run) if run.1 + 1 == n => run.1 = n,
            _ => runs.push((n, n)),
        }
    }
    let runs: Vec<String> = runs.iter().map(|(lo, hi)| format!("[{lo},{hi}]")).collect();
    format!("[{}]", runs.join(","))
}

// ──────────────────────────── the ceiling wrapper ────────────────────────────

/// A store that refuses any batch putting a value past [`MAX_VALUE_BYTES`] with a definite error,
/// as FoundationDB answers `2103 value_too_large`, and otherwise delegates to `inner`. It logs
/// every batch sent to it, refused or not, and can apply one blind batch to `inner` just before
/// it acts on the next commit. Fixture writes go to `inner` directly, so they are not logged.
struct Ceiling {
    inner: RedbMetadataStore,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Every batch sent, in order.
    sent: Vec<WriteBatch>,
    /// Applied to `inner` just before the next commit is acted on: another writer at that instant.
    before_next_commit: Option<WriteBatch>,
}

impl Ceiling {
    fn over(inner: RedbMetadataStore) -> Self {
        Self {
            inner,
            state: Mutex::new(State::default()),
        }
    }

    /// Every batch sent so far, and the log emptied.
    fn take_sent(&self) -> Vec<WriteBatch> {
        std::mem::take(&mut self.state.lock().unwrap().sent)
    }

    /// Apply `write` to the inner store just before the next commit.
    fn before_next_commit(&self, write: WriteBatch) {
        self.state.lock().unwrap().before_next_commit = Some(write);
    }
}

#[async_trait]
impl MetadataStore for Ceiling {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<ScanPage> {
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        let first = {
            let mut state = self.state.lock().unwrap();
            state.sent.push(batch.clone());
            state.before_next_commit.take()
        };
        if let Some(write) = first {
            assert_eq!(
                self.inner.commit(write).await?,
                CommitOutcome::Committed,
                "the other writer's batch lands"
            );
        }
        if let Some((key, value)) = batch.puts.iter().find(|(_, v)| v.len() > MAX_VALUE_BYTES) {
            return Err(BoxError::from(format!(
                "2103 value_too_large: {} is {} bytes, past {MAX_VALUE_BYTES}",
                String::from_utf8_lossy(key),
                value.len()
            )));
        }
        self.inner.commit(batch).await
    }
}

// ─────────────────────────────── the upload ───────────────────────────────

/// One session made by Create on a store with its bucket, with parts planted.
struct Upload {
    store: Ceiling,
    id: UploadId,
    object: String,
}

/// A store holding the bucket and one session, made by Create, with each of `parts` planted as a
/// committed part.
async fn upload(parts: &[u32]) -> Upload {
    let inner = RedbMetadataStore::in_memory().unwrap();
    let bucket = WriteBatch::new().put(metadata::bucket_key(BUCKET), MARKER);
    assert_eq!(
        inner.commit(bucket).await.unwrap(),
        CommitOutcome::Committed
    );
    let object = format!("{BUCKET}/object-1");
    let request = CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object: &object,
        content_type: None,
        created_at_millis: CREATED_AT,
        clock_source: CLOCK,
    };
    let identity = (
        UploadId::new(format!("{:032x}", 1)).unwrap(),
        SegmentNonce::new(format!("{:032x}", 0x5a5a_5a5b)).unwrap(),
    );
    let outcome = create_session(&inner, &profile_of(1), &request, &mut Fixed(Some(identity)))
        .await
        .expect("the create ran");
    let CreateOutcome::Created { upload_id: id } = outcome else {
        panic!("expected Created, got {outcome:?}");
    };
    for chunk in parts.chunks(PLANT_BATCH) {
        let mut plant = WriteBatch::new();
        for &n in chunk {
            let one = plant_part(&id, n);
            plant.puts.extend(one.puts);
        }
        assert_eq!(inner.commit(plant).await.unwrap(), CommitOutcome::Committed);
    }
    let up = Upload {
        store: Ceiling::over(inner),
        id,
        object,
    };
    let (epoch, attempts, _) = up.session().await;
    assert_eq!((epoch, attempts), (0, 0), "Open@0, no attempt spent");
    up
}

impl Upload {
    /// Complete this upload's own target naming `parts`, on the session's own clock.
    async fn complete(&self, parts: &NamedParts) -> Result<CompleteOutcome, CompleteError> {
        let request = CompleteRequest {
            bucket: BUCKET,
            parent: ROOT,
            object: &self.object,
            parts,
            fenced_at_millis: FENCED_AT,
            clock_source: CLOCK,
        };
        let publish = PublishRequest {
            fresh_inode: FRESH_INODE,
            completed_at_millis: COMPLETED_AT,
        };
        within(complete_upload(&self.store, &self.id, &request, &publish)).await
    }

    /// The session's epoch, attempts and stored bytes; it must be `Open`.
    async fn session(&self) -> (u64, u32, Bytes) {
        let bytes = self.store.inner.get(&mpu_key(&self.id)).await.unwrap();
        let bytes = bytes.expect("the session");
        let record = decode_session_record(&bytes).expect("a valid session");
        assert!(
            matches!(record.state(), SessionState::Open {}),
            "{:?}",
            record.state()
        );
        (record.epoch(), record.attempts(), bytes)
    }

    /// The `retire:<mode>:s:<id>:1` key the flip from the fence at epoch 1 installs under.
    fn retire_key(&self, mode: RetireMode) -> Vec<u8> {
        let token = RetireToken::Session {
            upload_id: self.id.clone(),
            epoch: FENCE_EPOCH,
            part: None,
        };
        retire_key(mode, &token)
    }

    /// The stored value of the flip's `{parts}` obligation in `mode`, if any.
    async fn obligation(&self, mode: RetireMode) -> Option<Bytes> {
        self.store.inner.get(&self.retire_key(mode)).await.unwrap()
    }

    /// Assert the flip's `{parts}` obligation in `mode` is stored as `parts_value(numbers)`, exactly
    /// `len` bytes and inside the ceiling, and decodes to that set.
    async fn assert_obligation(&self, mode: RetireMode, numbers: &[u32], len: usize) {
        let stored = self.obligation(mode).await.expect("the obligation");
        assert_eq!(stored.len(), len, "{mode:?}: the stated size");
        assert!(
            stored.len() <= MAX_VALUE_BYTES,
            "{mode:?}: inside the ceiling"
        );
        assert_eq!(
            stored.as_ref(),
            parts_value(numbers).as_bytes(),
            "{mode:?}: the parts, spelled"
        );
        let (_, _, payload) =
            decode_retire_obligation(&self.retire_key(mode), &stored).expect("it decodes");
        let set = PartNumberSet::from_numbers(numbers.iter().map(|&n| part(n))).unwrap();
        assert_eq!(payload.parts(), Some(&PartScope::Set(set)));
    }
}

// ─────────────────────────────── assertions ───────────────────────────────

/// The refusal `SessionTooLarge` naming `bytes` against the ceiling.
fn too_large(bytes: usize) -> Refusal {
    Refusal::SessionTooLarge {
        bytes: bytes as u64,
        limit: MAX_VALUE_BYTES as u64,
    }
}

/// The request was published.
fn assert_published(outcome: &Result<CompleteOutcome, CompleteError>) {
    assert!(
        matches!(outcome, Ok(CompleteOutcome::Published(_))),
        "expected Published, got {outcome:?}"
    );
}

/// The request was refused `refusal`.
fn assert_refused(outcome: Result<CompleteOutcome, CompleteError>, refusal: &Refusal) {
    match outcome {
        Ok(CompleteOutcome::Refused(got)) => assert_eq!(&got, refusal),
        other => panic!("expected Refused({refusal:?}), got {other:?}"),
    }
}

/// No batch sent put a value past the ceiling: the wrapper took every one.
fn assert_all_inside(sent: &[WriteBatch]) {
    for batch in sent {
        for (key, value) in &batch.puts {
            assert!(
                value.len() <= MAX_VALUE_BYTES,
                "{} is {} bytes",
                String::from_utf8_lossy(key),
                value.len()
            );
        }
    }
}

// ─────────────────────────────── the wrapper ───────────────────────────────

/// The wrapper refuses a value one byte past the ceiling, with nothing applied, and takes one at
/// it: the ceiling the legs below run against is the store's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_wrapper_refuses_a_value_past_the_ceiling_and_takes_one_at_it() {
    let store = Ceiling::over(RedbMetadataStore::in_memory().unwrap());
    let past = WriteBatch::new().put(b"probe".to_vec(), vec![b'x'; MAX_VALUE_BYTES + 1]);
    assert!(store.commit(past).await.is_err(), "past the ceiling");
    assert_eq!(store.inner.get(b"probe").await.unwrap(), None);
    let at = WriteBatch::new().put(b"probe".to_vec(), vec![b'x'; MAX_VALUE_BYTES]);
    assert_eq!(store.commit(at).await.unwrap(), CommitOutcome::Committed);
}

// ──────────────────── (I1) the widest sets inside S3's range ────────────────────

/// **(I1)** Parts 1 to 10,000 and a Complete naming the odd ones: the two sets are each 5,000
/// isolated part numbers, the most a set inside S3's range can hold. It publishes, and both
/// obligations are stored inside the ceiling: the published odd parts' `retire:records:` at
/// 58,901 bytes and the unnamed even parts' `retire:bytes:` at 58,909.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i1_inside_s3s_range_both_part_set_obligations_fit_and_it_publishes() {
    let all: Vec<u32> = (1..=10_000).collect();
    let up = upload(&all).await;

    let outcome = up.complete(&named(&odd_to(9_999))).await;

    assert_published(&outcome);
    assert_all_inside(&up.store.take_sent());
    up.assert_obligation(RetireMode::Records, &odd_to(9_999), ODD_TO_9_999_VALUE)
        .await;
    up.assert_obligation(RetireMode::Bytes, &even_to(10_000), EVEN_TO_10_000_VALUE)
        .await;
}

// ──────────────────── (I2) a sparse set past the ceiling ────────────────────

/// **(I2)** Parts 1, 3, …, 19,999 (10,000 of them) and a Complete naming them all: the published
/// parts' `{parts}` value would be 128,901 bytes. The request is refused `SessionTooLarge` naming
/// that size, with **zero** commits: no fence, so no attempt spent and no in-flight part losing
/// its session. The session is byte-for-byte as it was, and an Abort then fences it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i2_ten_thousand_sparse_parts_are_refused_with_zero_commits_and_stay_abortable() {
    let parts = odd_to(19_999);
    assert_eq!(parts.len(), 10_000);
    assert_eq!(parts_value(&parts).len(), ODD_TO_19_999_VALUE);
    let up = upload(&parts).await;
    let (_, _, before) = up.session().await;

    let outcome = up.complete(&named(&parts)).await;

    assert_refused(outcome, &too_large(ODD_TO_19_999_VALUE));
    assert!(up.store.take_sent().is_empty(), "zero commits");
    let (epoch, attempts, after) = up.session().await;
    assert_eq!((epoch, attempts), (0, 0));
    assert_eq!(after, before, "the session as it was");

    let aborted = within(abort_session(&up.store, &up.id)).await;
    assert!(matches!(aborted, Ok(AbortOutcome::Fenced)), "{aborted:?}");
    assert_all_inside(&up.store.take_sent());
}

// ──────────────────── (I3) the stored value, not the bare set ────────────────────

/// **(I3, inside)** Parts 1, 3, …, 15,869 (7,935 of them), a Complete naming all: the set encodes
/// to 99,981 bytes and its `{"parts":…}` value to 99,991, inside the ceiling, so it publishes,
/// and the installed obligation is stored at that length. No part is left unnamed, so no
/// `retire:bytes:` obligation is installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i3_a_value_just_inside_the_ceiling_publishes() {
    let parts = odd_to(15_869);
    assert_eq!(parts.len(), 7_935);
    assert_eq!(bare_set(&parts).len(), ODD_TO_15_869_SET);
    let up = upload(&parts).await;

    let outcome = up.complete(&named(&parts)).await;

    assert_published(&outcome);
    assert_all_inside(&up.store.take_sent());
    up.assert_obligation(RetireMode::Records, &parts, ODD_TO_15_869_VALUE)
        .await;
    assert_eq!(up.obligation(RetireMode::Bytes).await, None);
}

/// Parts whose `{"parts":…}` value is exactly [`MAX_VALUE_BYTES`]. Each isolated number costs
/// `[n,n]` and a comma, an even count of bytes, and the frame `{"parts":[…]}` less one comma is 11
/// more, so a set of isolated numbers always has a value of odd length. One run whose two ends
/// differ in width supplies the byte that makes it even: 1, 3, …, 9,997, the run 9,999 to
/// 10,000, then 10,002, 10,004, …, 15,862, then four six-digit numbers.
fn at_ceiling_parts() -> Vec<u32> {
    let mut parts = odd_to(9_997);
    parts.extend([9_999, 10_000]);
    parts.extend((10_002..=15_862).step_by(2));
    parts.extend((100_001..=100_007).step_by(2));
    parts
}

/// **(I3, at)** The ceiling's admissible side: a Complete naming [`at_ceiling_parts`], whose
/// `{"parts":…}` value lands exactly on [`MAX_VALUE_BYTES`], publishes, and the obligation is
/// stored at exactly that length. A value at the ceiling is one the store takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i3_a_value_exactly_at_the_ceiling_publishes() {
    let parts = at_ceiling_parts();
    assert_eq!(parts_value(&parts).len(), AT_CEILING_VALUE);
    let up = upload(&parts).await;

    let outcome = up.complete(&named(&parts)).await;

    assert_published(&outcome);
    assert_all_inside(&up.store.take_sent());
    up.assert_obligation(RetireMode::Records, &parts, AT_CEILING_VALUE)
        .await;
}

/// **(I3, past)** Parts 1, 3, …, 15,871 (7,936 of them), a Complete naming all: the bare set is
/// 99,995 bytes, inside the ceiling, but the `{"parts":…}` value the flip stores is 100,005, past
/// it. A check on the bare set would let it through to a flip that can never land; it is refused
/// `SessionTooLarge` naming the value's size, with zero commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i3_a_value_past_the_ceiling_is_refused_though_its_bare_set_fits() {
    let parts = odd_to(15_871);
    assert_eq!(parts.len(), 7_936);
    assert_eq!(bare_set(&parts).len(), ODD_TO_15_871_SET);
    assert_eq!(parts_value(&parts).len(), ODD_TO_15_871_VALUE);
    let up = upload(&parts).await;
    let (_, _, before) = up.session().await;

    let outcome = up.complete(&named(&parts)).await;

    assert_refused(outcome, &too_large(ODD_TO_15_871_VALUE));
    assert!(up.store.take_sent().is_empty(), "zero commits");
    let (_, _, after) = up.session().await;
    assert_eq!(after, before, "the session as it was");
}

// ──────────────────── (I4) a part added after the first read ────────────────────

/// The parts of (I4): part 2, which the request names, and 1, 3, …, 15,869, which it leaves
/// unnamed. The unnamed set is (I3)'s, 99,991 bytes as a value.
fn i4_parts() -> Vec<u32> {
    let mut parts = odd_to(15_869);
    parts.push(2);
    parts.sort_unstable();
    parts
}

/// **(I4, control)** Without another writer, (I4)'s request publishes: the unnamed parts'
/// `retire:bytes:` value is 99,991 bytes, inside the ceiling, so the set the Complete first reads
/// fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_control_the_set_first_read_fits_and_publishes() {
    let up = upload(&i4_parts()).await;

    let outcome = up.complete(&named(&[2])).await;

    assert_published(&outcome);
    assert_all_inside(&up.store.take_sent());
    up.assert_obligation(RetireMode::Records, &[2], parts_value(&[2]).len())
        .await;
    up.assert_obligation(RetireMode::Bytes, &odd_to(15_869), ODD_TO_15_869_VALUE)
        .await;
}

/// **(I4)** As the control, but the wrapper plants part 15,871, unnamed, just before the fence
/// commits: the frozen set's unnamed parts are then 100,005 bytes as a value. The check before the
/// fence passed, so the request fences; the check against the frozen set refuses it
/// `SessionTooLarge` naming that size, and the fence is released. Two commits, the fence and its
/// release; the session ends `Open@2` with `attempts` given back, and nothing is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_a_part_added_before_the_fence_is_refused_against_the_frozen_set_and_released() {
    let up = upload(&i4_parts()).await;
    up.store.before_next_commit(plant_part(&up.id, 15_871));

    let outcome = up.complete(&named(&[2])).await;

    assert_refused(outcome, &too_large(ODD_TO_15_871_VALUE));
    let sent = up.store.take_sent();
    assert_eq!(sent.len(), 2, "the fence and its release");
    let session_put = |batch: &WriteBatch| {
        let puts: Vec<_> = batch.puts.iter().map(|(key, _)| key.clone()).collect();
        assert_eq!(puts, vec![mpu_key(&up.id)], "only the session is put");
        decode_session_record(&batch.puts[0].1).expect("a valid session")
    };
    let fence = session_put(&sent[0]);
    assert!(
        matches!(fence.state(), SessionState::Completing { .. }),
        "{:?}",
        fence.state()
    );
    assert_eq!((fence.epoch(), fence.attempts()), (FENCE_EPOCH, 1));
    let released = session_put(&sent[1]);
    assert_eq!(
        (released.epoch(), released.attempts()),
        (FENCE_EPOCH + 1, 0)
    );
    let (epoch, attempts, _) = up.session().await;
    assert_eq!(
        (epoch, attempts),
        (FENCE_EPOCH + 1, 0),
        "Open@2, attempts restored"
    );
    assert_eq!(up.obligation(RetireMode::Records).await, None);
    assert_eq!(up.obligation(RetireMode::Bytes).await, None);
    let dirent = metadata::dirent_key(ROOT, &up.object);
    let dirent = up.store.inner.get(&dirent).await.unwrap();
    assert_eq!(dirent, None, "nothing published");
}

/// **(I4, already past)** As (I4), but part 15,871 is there from the start, with no other writer:
/// the request names only part 2, so its published set is a few bytes, and it is the *unnamed*
/// parts' `retire:bytes:` value, 100,005 bytes, that is past the ceiling when the Complete first
/// reads the set. The check before the fence measures that obligation too, so the request is
/// refused `SessionTooLarge` naming that size with **zero** commits: no fence, no release, the
/// session byte-for-byte as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_an_unnamed_set_already_past_the_ceiling_is_refused_with_zero_commits() {
    let mut parts = i4_parts();
    parts.push(15_871);
    assert_eq!(parts_value(&odd_to(15_871)).len(), ODD_TO_15_871_VALUE);
    assert_eq!(
        parts_value(&[2]),
        r#"{"parts":[[2,2]]}"#,
        "the published set"
    );
    let up = upload(&parts).await;
    let (_, _, before) = up.session().await;

    let outcome = up.complete(&named(&[2])).await;

    assert_refused(outcome, &too_large(ODD_TO_15_871_VALUE));
    assert!(up.store.take_sent().is_empty(), "zero commits");
    let (epoch, attempts, after) = up.session().await;
    assert_eq!((epoch, attempts), (0, 0), "Open@0, no attempt spent");
    assert_eq!(after, before, "the session as it was");
    assert_eq!(up.obligation(RetireMode::Records).await, None);
    assert_eq!(up.obligation(RetireMode::Bytes).await, None);
}
