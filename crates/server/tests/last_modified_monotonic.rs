//! A file's stored `modified` strictly increases across overwrites, and no S3 response shows a
//! `Last-Modified` later than its own `Date` (issue #768).
//!
//! Each gateway of an active/active fleet stamps `modified` from its own clock. Before #768 an
//! overwrite stored the publisher's stamp as given, so a gateway whose clock was behind the one
//! that wrote the prior generation moved `Last-Modified` backwards while the content moved
//! forwards, and a sync tool that compares size and `Last-Modified` silently skipped a same-size
//! overwrite. And the S3 surface showed the stored time as given, so a stamp from a fast clock
//! was served as a future `Last-Modified`, which RFC 9110 §8.8.2.1 forbids. The rules pinned
//! here:
//!
//! * **stored:** an overwrite that records a time commits
//!   `max(publishing clock, prior modified + 1 ms)`, saturating, with no cap and no automatic
//!   correction; an overwrite with no time over a version that has one is refused with a typed
//!   error and writes nothing;
//! * **shown:** every S3 `Last-Modified` (header, listing, `304`) and every date precondition
//!   uses `min(stored, serving gateway's clock)`, so it is never later than the response's
//!   `Date`; a stored time in the past is shown as stored.
//!
//! Legs (1)–(6) read the **stored** value (`ObjectGateway::head_object` and core), never a
//! rendered one:
//!
//! 1. a streaming overwrite over a prior an hour **ahead** (mid-second) commits exactly
//!    `prior + 1` and the new ETag — not the next whole second, not `+1 s`, not `u64::MAX`;
//! 2. the same through the buffered `Gateway::put_object`;
//! 3. over a prior an hour in the past, an overwrite records the current clock (both paths);
//! 4. a create records the current clock (both paths);
//! 5. both public core overwrite commits, with an explicit publisher stamp, commit the rule's
//!    exact values, refuse an unstamped overwrite of a stamped version with the typed error and
//!    leave the record byte-for-byte unchanged, and keep three chains ordered;
//! 6. over a prior **two days** ahead, an overwrite still records exactly `prior + 1` (no heal),
//!    on both paths.
//!
//! Legs (7) and (8) drive the real S3 surface (`S3Gateway` over an in-process `Gateway` on a
//! loopback listener) with signed raw HTTP/1.1, reading `Date` and `Last-Modified` from the SAME
//! response. A shown time is **in the window** when `before ≤ shown ≤ Date ≤ after` in whole
//! seconds, `before` and `after` being this test's clock read just before sending and just after
//! the response arrived. The `Date` bound pins RFC 9110 §8.8.2.1; the `after` bound pins that
//! the shown time and the `Date` are this host's clock and not a clock running ahead of it (the
//! handler writes `Date` from the same reading, so `shown ≤ Date` alone would hold for any
//! reading):
//!
//! 7. a file stored an hour ahead shows in the window on HEAD, GET (unranged `200` and ranged
//!    `206`), the listing and a `304`, passes `If-Unmodified-Since` / answers `If-Modified-Since`
//!    with `304` for an instant before the stored time on both GET arms, before and after a
//!    same-size overwrite; a file stored an hour behind is shown exactly as stored; after the
//!    overwrite the shown time is earlier than the seed (the documented mirror exception is
//!    real);
//! 8. on one healthy gateway, a create and three back-to-back same-size overwrites each show in
//!    the window (no visible lead) and never earlier than before.
//!
//! Real two-host skew is not reproducible in-process (`now_millis` reads the host clock), so the
//! fast-clock legs seed a prior whose stamp is ahead of this host's clock: exactly the record a
//! fast-clocked gateway leaves behind, since the store keeps no record of which gateway wrote a
//! stamp. Seeding follows `segmented_object_read.rs` (`write_flat`): the prior generation is
//! written through `wyrd_core::write` before the store moves into `Gateway::new`. The S3 legs
//! follow `s3_list_objects.rs` (`seed_bucket`, `start_gateway`, signed raw requests) and
//! `s3_last_modified_agreement.rs` (the dirent `"{bucket}/{key}"`, the SDK's own date parsers).
//!
//! Every symbol used here predates #768, so on the reverted base this file compiles and fails
//! by assertion. The typed refusal is recognised by its `Debug` rendering for that reason (the
//! type itself is new); `crates/core`'s unit tests downcast to it by name.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): the stored-value legs bracket the gateway's own wall-clock
// stamp (`now_millis`) and the wire legs bracket the serving gateway's wall-clock cap, so this
// file must read the same source those lifecycles use; SigV4 request dates use it too. Leg (5)
// passes explicit stamps and reads no clock (#619).
// File scope (not per-site) is deliberate: a test crate never ships, so no production
// lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_smithy_types::date_time::Format;
use aws_smithy_types::DateTime;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, encode, inode_key, EcScheme, InodeRecord, InodeState, ObjectMeta};
use wyrd_core::write;
use wyrd_gateway_core::{ContentHash, ObjectGateway};
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials as GatewayCredentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

const ROOT: u64 = 0;
/// The logical lease instant the seeding writes use (as the peer harnesses do).
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const CHUNK: usize = 4;

const SECOND: u64 = 1_000;
const MINUTE: u64 = 60 * SECOND;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

/// The key legs (1)–(6) address through `ObjectGateway`.
const KEY: &str = "doc";
/// Same-size bodies: a same-size overwrite is the case a size-and-mtime sync tool misses.
const OLD: &[u8] = b"old-bytes-of-file";
const NEW: &[u8] = b"NEW-BYTES-OF-FILE";
const NEWER: &[u8] = b"newer-file-bytes!";
const FOURTH: &[u8] = b"fourth-file-bytes";

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "stamps";
/// Stored an hour ahead of this host's clock (leg 7).
const AHEAD: &str = "ahead";
/// Stored an hour behind it (leg 7).
const BEHIND: &str = "behind";
/// Upper bound on every await on the store or the socket: a stalled gateway, or one that never
/// closes the connection, fails the test instead of hanging it.
const DEADLINE: Duration = Duration::from_secs(30);

/// Await `fut` for at most [`DEADLINE`], failing the test (not hanging it) past that. Every
/// helper below that awaits the store, the gateway or the socket goes through it.
async fn bounded<F: Future>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(DEADLINE, fut)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {DEADLINE:?}"))
}

/// This host's wall clock in epoch millis: the source the gateway's `now_millis` and the S3
/// gateway's shown-time cap both read, so readings taken around a call bracket them.
fn wall_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as u64
}

/// This host's wall clock in whole epoch seconds, rounded down.
fn wall_secs() -> u64 {
    wall_millis() / SECOND
}

/// Half-way through the second containing `t`. A mid-second prior makes `prior + 1`,
/// `prior + 1000` and "the next whole second" three different answers.
fn mid_second(t: u64) -> u64 {
    t / SECOND * SECOND + 500
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = FsChunkStore::open(dir.path()).expect("fs chunk store");
    (meta, chunks, dir)
}

/// Publish `data` as the file `name` (a dirent under `ROOT`) through the ordinary write path,
/// with the content's real ETag and `modified` stamped as the prior generation's, exactly where
/// the gateway's own PUT stamps it.
async fn seed_file(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
    inode: u64,
    name: &str,
    data: &[u8],
    modified: u64,
) {
    let mut next = u128::from(inode) << 16;
    let mut plan = write::plan_write(data, CHUNK, EcScheme::None, || {
        next += 1;
        next
    })
    .expect("plan");
    plan.object_meta = ObjectMeta {
        etag: Some(sha256_hex(data)),
        content_type: None,
        modified: Some(modified),
    };
    bounded(&format!("seeding {name}"), async {
        write::intent(meta, &plan, NOW + TTL).await.expect("intent");
        write::write_fragments(chunks, &plan)
            .await
            .expect("fan out");
        assert_eq!(
            write::commit_create(meta, ROOT, name, inode, &plan, NOW)
                .await
                .expect("commit"),
            CommitOutcome::Committed
        );
        write::release(meta, &plan).await.expect("release");
    })
    .await;
}

type TestGateway = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

fn gateway(meta: RedbMetadataStore, chunks: FsChunkStore) -> TestGateway {
    Gateway::new(meta, chunks, MemCoordination::new()).with_chunk_size(CHUNK)
}

/// A gateway over a store holding `KEY` stamped `modified` (or nothing, for a create).
async fn gateway_over(prior: Option<u64>) -> (TestGateway, tempfile::TempDir) {
    let (meta, chunks, dir) = backends();
    if let Some(modified) = prior {
        seed_file(&meta, &chunks, 1_001, KEY, OLD, modified).await;
    }
    (gateway(meta, chunks), dir)
}

/// PUT `data` under `KEY` through one of the two gateway PUT paths; returns the new ETag.
async fn put(gw: &TestGateway, buffered: bool, data: &'static [u8]) -> String {
    bounded(&format!("the {} PUT", path_name(buffered)), async {
        if buffered {
            gw.put_object(KEY, data).await.expect("buffered put");
            sha256_hex(data)
        } else {
            let source = futures_util::stream::iter([Ok(Bytes::from_static(data))]);
            gw.put_object_streaming(
                KEY,
                source,
                data.len() as u64,
                ContentHash::Unverified,
                None,
            )
            .await
            .expect("streaming put")
        }
    })
    .await
}

/// The **stored** metadata of `KEY`, as `ObjectGateway::head_object` hands it to every gateway.
async fn stored(gw: &TestGateway) -> wyrd_gateway_core::ObjectMeta {
    bounded("head_object", gw.head_object(KEY))
        .await
        .expect("head")
        .expect("the file is committed")
}

fn path_name(buffered: bool) -> &'static str {
    if buffered {
        "buffered"
    } else {
        "streaming"
    }
}

/// An overwrite over a prior stamped `seed` must commit exactly `seed + 1` with the new ETag.
async fn assert_overwrite_steps_one_millisecond_past(buffered: bool, seed: u64, what: &str) {
    assert_eq!(OLD.len(), NEW.len(), "a same-size overwrite");
    let (gw, _dir) = gateway_over(Some(seed)).await;
    let etag = put(&gw, buffered, NEW).await;
    let after = stored(&gw).await;
    assert_eq!(
        after.modified,
        Some(seed + 1),
        "{} overwrite over a prior {what} (stored {seed}): the stored time must step exactly \
         1 ms past the prior, never back to this host's clock",
        path_name(buffered),
    );
    assert_eq!(after.etag.as_deref(), Some(etag.as_str()));
    assert_eq!(after.etag, Some(sha256_hex(NEW)), "the new content's ETag");
    assert_eq!(
        bounded("get_object", gw.get_object(KEY))
            .await
            .unwrap()
            .as_deref(),
        Some(NEW)
    );
}

/// (1) Streaming PUT after a fast clock.
#[tokio::test]
async fn streaming_overwrite_after_a_fast_clock_steps_one_millisecond_past_the_prior() {
    let seed = mid_second(wall_millis() + HOUR);
    assert_overwrite_steps_one_millisecond_past(false, seed, "an hour ahead").await;
}

/// (2) The same through the buffered `Gateway::put_object`.
#[tokio::test]
async fn buffered_overwrite_after_a_fast_clock_steps_one_millisecond_past_the_prior() {
    let seed = mid_second(wall_millis() + HOUR);
    assert_overwrite_steps_one_millisecond_past(true, seed, "an hour ahead").await;
}

/// (6) No heal through the gateway: a prior two days ahead (a badly wrong clock) is still
/// ordered after, on both PUT paths. A one-day cap that replaced it with the clock fails here.
#[tokio::test]
async fn overwrite_over_a_prior_two_days_ahead_is_still_ordered_after_it() {
    for buffered in [false, true] {
        let seed = mid_second(wall_millis() + 2 * DAY);
        assert_overwrite_steps_one_millisecond_past(buffered, seed, "two days ahead").await;
    }
}

/// (3) Over a prior an hour in the past an overwrite records the current clock, on both paths.
#[tokio::test]
async fn overwrite_over_a_past_prior_records_the_current_clock() {
    for buffered in [false, true] {
        let (gw, _dir) = gateway_over(Some(mid_second(wall_millis() - HOUR))).await;
        let before = wall_millis();
        put(&gw, buffered, NEW).await;
        let after = wall_millis();
        let modified = stored(&gw).await.modified.expect("stamped");
        assert!(
            (before..=after).contains(&modified),
            "{}: {modified} must be the clock, within [{before}, {after}]",
            path_name(buffered),
        );
    }
}

/// (4) A create has no prior and records the current clock, as before, on both paths.
#[tokio::test]
async fn create_records_the_current_clock() {
    for buffered in [false, true] {
        let (gw, _dir) = gateway_over(None).await;
        let before = wall_millis();
        put(&gw, buffered, NEW).await;
        let after = wall_millis();
        let modified = stored(&gw).await.modified.expect("stamped");
        assert!(
            (before..=after).contains(&modified),
            "{}: {modified} must be the clock, within [{before}, {after}]",
            path_name(buffered),
        );
    }
}

// ---- (5) the rule's exact values, on both public core overwrite commits ----

/// The publisher's stamp in leg (5): 2023-11-14T22:13:20.250Z, a quarter into a second.
const T: u64 = 1_700_000_000_250;
const MAX: u64 = u64::MAX;
const ID: u64 = 7;

#[derive(Clone, Copy, Debug)]
enum Api {
    /// `metadata::commit_chunk_map_superseding`.
    Superseding,
    /// `metadata::commit_chunk_map_superseding_leased`, the one `write::commit_overwrite` calls.
    SupersedingLeased,
}

/// What one overwrite must do.
#[derive(Clone, Copy, Debug)]
enum Expect {
    /// Commit, storing exactly this `modified`.
    Commits(Option<u64>),
    /// Return the typed refusal and leave the stored record byte-for-byte unchanged.
    Refused,
}
use Expect::{Commits, Refused};

/// Seed a committed record with the given `modified`.
async fn seed_record(store: &RedbMetadataStore, modified: Option<u64>) {
    let record = InodeRecord {
        size: 0,
        chunk_map: Vec::new().into(),
        state: InodeState::Committed,
        version: 1,
        etag: Some("prior".to_owned()),
        content_type: None,
        modified,
    };
    assert_eq!(
        bounded(
            "seeding the record",
            store.commit(WriteBatch::new().put(inode_key(ID), encode(&record))),
        )
        .await
        .expect("seed"),
        CommitOutcome::Committed
    );
}

/// The stored bytes of the record.
async fn record_bytes(store: &RedbMetadataStore) -> Bytes {
    bounded("reading the record", store.get(&inode_key(ID)))
        .await
        .expect("get")
        .expect("the record exists")
}

/// Overwrite the record through `api` with publisher stamp `published`, then check `expect`.
/// A mismatch is returned as a description, so a run reports every case that broke.
async fn overwrite(
    store: &RedbMetadataStore,
    api: Api,
    published: Option<u64>,
    expect: Expect,
) -> Result<(), String> {
    let before = record_bytes(store).await;
    let prior: InodeRecord = metadata::decode(&before).expect("decode the prior");
    let meta = ObjectMeta {
        etag: Some("next".to_owned()),
        content_type: None,
        modified: published,
    };
    let result = bounded(&format!("the {api:?} commit"), async {
        match api {
            Api::Superseding => {
                metadata::commit_chunk_map_superseding(store, ID, &prior, vec![], 0, 0, &meta).await
            }
            // An empty pending set needs no leases.
            Api::SupersedingLeased => {
                metadata::commit_chunk_map_superseding_leased(
                    store,
                    ID,
                    &prior,
                    vec![],
                    0,
                    0,
                    &[],
                    0,
                    &meta,
                )
                .await
            }
        }
    })
    .await;
    let after = record_bytes(store).await;
    let stored: InodeRecord = metadata::decode(&after).expect("decode the stored record");
    match (expect, result) {
        (Commits(want), Ok(CommitOutcome::Committed)) if stored.modified == want => Ok(()),
        (Commits(want), Ok(CommitOutcome::Committed)) => Err(format!(
            "committed modified {:?}, want {want:?}",
            stored.modified
        )),
        (Commits(_), other) => Err(format!("did not commit: {other:?}")),
        (Refused, Err(err)) => {
            let typed = format!("{err:?}");
            if !typed.starts_with("UnstampedOverwrite") {
                Err(format!("refused, but not with the typed refusal: {typed}"))
            } else if after != before {
                Err("refused, but the stored record changed".to_owned())
            } else if (stored.modified, stored.version) != (prior.modified, prior.version) {
                Err("refused, but modified or version moved".to_owned())
            } else {
                Ok(())
            }
        }
        (Refused, Ok(outcome)) => Err(format!(
            "an unstamped overwrite of a stamped version must be refused with a typed error, \
             not answer {outcome:?} (stored modified now {:?})",
            stored.modified
        )),
    }
}

/// Seed a fresh record with `prior`, then run each `(publisher stamp, expected)` overwrite in
/// order, stopping the chain at its first mismatch.
async fn chain(
    api: Api,
    case: &str,
    prior: Option<u64>,
    steps: &[(Option<u64>, Expect)],
    failures: &mut Vec<String>,
) {
    let store = RedbMetadataStore::in_memory().expect("in-memory redb");
    seed_record(&store, prior).await;
    for (step, (published, expect)) in steps.iter().enumerate() {
        if let Err(why) = overwrite(&store, api, *published, *expect).await {
            failures.push(format!(
                "{api:?}: {case} (prior {prior:?}), overwrite {} with publisher {published:?}: {why}",
                step + 1
            ));
            return;
        }
    }
}

/// (5) The rule's exact values, through BOTH public overwrite commits.
#[tokio::test]
async fn core_overwrite_commits_apply_the_rule_exactly() {
    let single: [(&str, Option<u64>, Option<u64>, Expect); 14] = [
        (
            "equal: two writes in one millisecond",
            Some(T),
            Some(T),
            Commits(Some(T + 1)),
        ),
        (
            "prior ahead, in the same second",
            Some(T + 500),
            Some(T),
            Commits(Some(T + 501)),
        ),
        (
            "prior ahead",
            Some(T + 5_000),
            Some(T),
            Commits(Some(T + 5_001)),
        ),
        (
            "prior one millisecond behind",
            Some(T - 1),
            Some(T),
            Commits(Some(T)),
        ),
        ("prior behind", Some(T - 5_000), Some(T), Commits(Some(T))),
        ("legacy prior", None, Some(T), Commits(Some(T))),
        ("neither has a time", None, None, Commits(None)),
        (
            "unstamped over a stamped prior",
            Some(T + 5_000),
            None,
            Refused,
        ),
        (
            "unstamped over a prior at u64::MAX",
            Some(MAX),
            None,
            Refused,
        ),
        (
            "no cap, no heal: prior two days ahead",
            Some(T + 2 * DAY),
            Some(T),
            Commits(Some(T + 2 * DAY + 1)),
        ),
        (
            "no heal at the top of the range",
            Some(MAX),
            Some(T),
            Commits(Some(MAX)),
        ),
        (
            "saturation: prior at max",
            Some(MAX),
            Some(MAX - 1),
            Commits(Some(MAX)),
        ),
        (
            "saturation: both one below max",
            Some(MAX - 1),
            Some(MAX - 1),
            Commits(Some(MAX)),
        ),
        (
            "saturation: both at max",
            Some(MAX),
            Some(MAX),
            Commits(Some(MAX)),
        ),
    ];
    let mut failures = Vec::new();
    for api in [Api::Superseding, Api::SupersedingLeased] {
        for (case, prior, published, expect) in single {
            chain(api, case, prior, &[(published, expect)], &mut failures).await;
        }
        chain(
            api,
            "chain: repeated overwrites in one clock millisecond",
            Some(T),
            &[
                (Some(T), Commits(Some(T + 1))),
                (Some(T), Commits(Some(T + 2))),
                (Some(T), Commits(Some(T + 3))),
            ],
            &mut failures,
        )
        .await;
        chain(
            api,
            "chain: a far-ahead prior is carried, pinned as intended",
            Some(T + 2 * DAY),
            &[
                (Some(T), Commits(Some(T + 2 * DAY + 1))),
                (Some(T), Commits(Some(T + 2 * DAY + 2))),
            ],
            &mut failures,
        )
        .await;
        chain(
            api,
            "chain: no reset through an unstamped overwrite",
            Some(T + 5_000),
            &[(None, Refused), (Some(T), Commits(Some(T + 5_001)))],
            &mut failures,
        )
        .await;
    }
    assert!(
        failures.is_empty(),
        "{} overwrite case(s) broke the rule:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ---- (7) and (8): the shown time, over the real S3 wire ----

/// Seed a `bucket:{name}` existence marker as raw key/value bytes, as
/// `s3_list_objects.rs::seed_bucket` does.
async fn seed_bucket(meta: &RedbMetadataStore, name: &str) {
    let key = format!("bucket:{name}").into_bytes();
    let value = format!("{{\"name\":\"{name}\",\"created_millis\":0}}").into_bytes();
    bounded(
        "seeding the bucket marker",
        meta.commit(WriteBatch::new().put(key, value)),
    )
    .await
    .expect("commit bucket marker");
}

/// The running S3 gateway. Dropping it aborts the serve task, so no helper task outlives the
/// test; it also keeps the chunk store's temp dir alive for as long as the gateway runs.
struct Server {
    addr: SocketAddr,
    task: JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start the S3 gateway on an ephemeral loopback port over a store holding the bucket marker
/// and each `(key, modified)` file, all seeded before the store moves into `Gateway::new`.
async fn start_s3(files: &[(&str, u64)]) -> Server {
    let (meta, chunks, dir) = backends();
    seed_bucket(&meta, BUCKET).await;
    for (inode, (key, modified)) in (2_001..).zip(files) {
        seed_file(
            &meta,
            &chunks,
            inode,
            &format!("{BUCKET}/{key}"),
            OLD,
            *modified,
        )
        .await;
    }
    let gateway = Arc::new(gateway(meta, chunks));
    let config = S3Config::new(vec![GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = bounded("binding the listener", TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(gateway, config);
    let task = tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    Server {
        addr,
        task,
        _dir: dir,
    }
}

/// A parsed response: status, header fields (lowercased names), and the de-framed body.
#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    /// The value of header `name` (lowercase), or `None` when absent. A repeated field fails
    /// the test: a response with two `Date` or `Last-Modified` values has no single answer.
    fn header(&self, name: &str) -> Option<&str> {
        let mut values = self.headers.iter().filter(|(n, _)| n == name);
        let first = values.next().map(|(_, v)| v.as_str());
        assert!(values.next().is_none(), "header {name} is repeated");
        first
    }

    fn text(&self) -> String {
        String::from_utf8(self.body.clone()).expect("UTF-8 response body")
    }

    /// This response's `Date`, in whole epoch seconds. RFC 9110 §6.6.1 requires an origin
    /// server with a clock to send it.
    fn date_secs(&self, what: &str) -> u64 {
        let date = self
            .header("date")
            .unwrap_or_else(|| panic!("{what}: the response carries no Date"));
        http_date_secs(date, what)
    }

    /// This response's `Last-Modified`, in whole epoch seconds.
    fn last_modified_secs(&self, what: &str) -> u64 {
        let shown = self
            .header("last-modified")
            .unwrap_or_else(|| panic!("{what}: the response carries no Last-Modified"));
        http_date_secs(shown, what)
    }
}

/// Parse an HTTP-date exactly as `aws-sdk-s3` decodes a `Last-Modified` header
/// (`Format::HttpDate`): the parser is the SDK's, not the gateway's.
fn http_date_secs(value: &str, what: &str) -> u64 {
    let parsed = DateTime::from_str(value, Format::HttpDate)
        .unwrap_or_else(|e| panic!("{what}: {value:?} is not an HTTP-date: {e}"));
    u64::try_from(parsed.secs()).expect("after the epoch")
}

/// Render whole epoch seconds as an IMF-fixdate with the SDK's formatter, independent of the
/// gateway's.
fn http_date(secs: u64) -> String {
    DateTime::from_secs(secs as i64)
        .fmt(Format::HttpDate)
        .expect("formattable instant")
}

/// The `<LastModified>` of the one `<Contents>` row whose `<Key>` is `key`, as epoch millis,
/// parsed as `aws-sdk-s3` parses that element (`Format::DateTimeWithOffset`, which keeps any
/// fraction). A missing or repeated row or element fails loudly.
fn listed_millis(body: &str, key: &str) -> u64 {
    let wanted = format!("<Key>{key}</Key>");
    let rows: Vec<&str> = body
        .split("<Contents>")
        .skip(1)
        .filter_map(|row| row.split_once("</Contents>").map(|(row, _)| row))
        .filter(|row| row.contains(&wanted))
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "exactly one <Contents> row for {key}: {body}"
    );
    let pieces: Vec<&str> = rows[0].split("<LastModified>").skip(1).collect();
    assert_eq!(
        pieces.len(),
        1,
        "exactly one <LastModified> for {key}: {body}"
    );
    let value = pieces[0]
        .split_once("</LastModified>")
        .unwrap_or_else(|| panic!("unclosed <LastModified> for {key}: {body}"))
        .0;
    let parsed = DateTime::from_str(value, Format::DateTimeWithOffset)
        .unwrap_or_else(|e| panic!("<LastModified> {value:?} for {key} is not RFC 3339: {e}"));
    u64::try_from(parsed.to_millis().expect("fits in millis")).expect("after the epoch")
}

/// Decode an HTTP/1.1 chunked body: size lines in hex, each chunk followed by CRLF, ended by
/// the zero-size chunk and an empty trailer section.
fn dechunk(mut raw: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end = raw
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("chunk-size line without CRLF")?;
        let size_field = raw[..line_end]
            .split(|&b| b == b';')
            .next()
            .unwrap_or_default();
        let size = std::str::from_utf8(size_field)
            .ok()
            .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit()))
            .and_then(|s| usize::from_str_radix(s, 16).ok())
            .ok_or("invalid chunk size")?;
        raw = &raw[line_end + 2..];
        if size == 0 {
            return if raw == b"\r\n" {
                Ok(out)
            } else {
                Err("the terminal chunk is not followed by exactly one CRLF".to_owned())
            };
        }
        if raw.len() < size + 2 || &raw[size..size + 2] != b"\r\n" {
            return Err(format!("chunk of {size} bytes truncated or unterminated"));
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..];
    }
}

/// Parse one complete HTTP/1.1 response read to connection close. A HEAD or `304` carries no
/// body; any other response is framed by `Content-Length` (exact) or chunked encoding.
fn parse_response(raw: &[u8], head: bool) -> Result<Response, String> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no end of the header block")?;
    let block = std::str::from_utf8(&raw[..split]).map_err(|_| "header block is not UTF-8")?;
    let mut lines = block.split("\r\n");
    let status_line = lines.next().ok_or("no status line")?;
    let status: u16 = status_line
        .strip_prefix("HTTP/1.1 ")
        .and_then(|rest| rest.get(..3))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("malformed status line: {status_line:?}"))?;
    let mut headers = Vec::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| format!("header line without a colon: {line:?}"))?;
        headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
    }
    let after = &raw[split + 4..];
    let field = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    };
    let body = if head || status == 304 {
        if !after.is_empty() {
            return Err(format!(
                "a body-less response carried {} bytes",
                after.len()
            ));
        }
        Vec::new()
    } else if field("transfer-encoding").is_some_and(|te| te.eq_ignore_ascii_case("chunked")) {
        dechunk(after)?
    } else if let Some(len) = field("content-length") {
        let len: usize = len.parse().map_err(|_| format!("Content-Length {len:?}"))?;
        if after.len() != len {
            return Err(format!(
                "Content-Length {len} but {} body bytes arrived",
                after.len()
            ));
        }
        after.to_vec()
    } else {
        return Err("no framing: neither Content-Length nor chunked".to_owned());
    };
    Ok(Response {
        status,
        headers,
        body,
    })
}

/// Send a SigV4-signed raw `{method} {path}[?{query}]` with `body` and the unsigned `extra`
/// headers, and parse the response. A listing carries a query, so the canonical query is
/// signed (`s3_list_objects.rs::signed_headers_q`). The whole exchange runs under [`DEADLINE`].
async fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> Response {
    let host = addr.to_string();
    let creds = GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let signed = sign(
        method,
        path,
        query,
        &host,
        &format_amz_date(SystemTime::now()),
        body,
        &creds,
        REGION,
        "s3",
    );
    let target = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let mut request = format!("{method} {target} HTTP/1.1\r\n");
    request.push_str(&format!("host: {host}\r\n"));
    request.push_str(&format!("authorization: {}\r\n", signed.authorization));
    request.push_str(&format!("x-amz-date: {}\r\n", signed.amz_date));
    request.push_str(&format!(
        "x-amz-content-sha256: {}\r\n",
        signed.content_sha256
    ));
    for (name, value) in extra {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!("content-length: {}\r\n", body.len()));
    request.push_str("connection: close\r\n\r\n");

    let raw = bounded(&format!("{method} {target}"), async {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request head");
        stream.write_all(body).await.expect("write request body");
        stream.flush().await.expect("flush");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read response");
        raw
    })
    .await;
    parse_response(&raw, method == "HEAD").unwrap_or_else(|e| {
        panic!(
            "{method} {target}: malformed response ({e}): {:?}",
            String::from_utf8_lossy(&raw)
        )
    })
}

fn object_path(key: &str) -> String {
    format!("/{BUCKET}/{key}")
}

/// The test's clock readings around one exchange, in whole seconds: `before` just before the
/// request was sent, `after` just after its response had arrived.
#[derive(Clone, Copy, Debug)]
struct Around {
    before: u64,
    after: u64,
}

/// Assert `before ≤ shown ≤ date ≤ after` (whole seconds) and return `shown`.
fn assert_in_window(what: &str, shown: u64, around: Around, date: u64) -> u64 {
    let Around { before, after } = around;
    assert!(
        before <= shown && shown <= date,
        "{what}: the shown time {shown} ({}) must lie between the clock just before the \
         request ({before}) and the response's own Date ({date}); a later one is a future \
         Last-Modified, which RFC 9110 §8.8.2.1 forbids",
        http_date(shown),
    );
    // The serving gateway reads the same host clock this test does, and read it before the
    // response arrived, so neither the shown time nor the `Date` it was capped at can be later
    // than the clock just after the response. A later value is a gateway clock reading that ran
    // ahead of the host's: `shown ≤ Date` alone would still hold for it, since the handler
    // writes `Date` from that same reading.
    assert!(
        date <= after,
        "{what}: the response's Date {date} ({}) must not be later than this host's clock just \
         after the response ({after}): the gateway's clock reading ran ahead of the host's",
        http_date(date),
    );
    assert!(
        shown <= after,
        "{what}: the shown time {shown} ({}) must not be later than this host's clock just \
         after the response ({after}): the gateway's clock reading ran ahead of the host's",
        http_date(shown),
    );
    shown
}

/// Send one request and check its `Last-Modified` header lies in its own window.
async fn shown_in_window(
    addr: SocketAddr,
    what: &str,
    method: &str,
    key: &str,
    extra: &[(&str, &str)],
    want_status: u16,
) -> (u64, Response) {
    let before = wall_secs();
    let response = send(addr, method, &object_path(key), "", extra, b"").await;
    let after = wall_secs();
    assert_eq!(response.status, want_status, "{what}: status");
    let shown = assert_in_window(
        what,
        response.last_modified_secs(what),
        Around { before, after },
        response.date_secs(what),
    );
    (shown, response)
}

/// ListObjectsV2, returning the listing and the clock readings around it.
async fn list(addr: SocketAddr, what: &str) -> (Response, Around) {
    let before = wall_secs();
    let listing = send(addr, "GET", &format!("/{BUCKET}"), "list-type=2", &[], b"").await;
    let after = wall_secs();
    assert_eq!(listing.status, 200, "{what}: listing status");
    (listing, Around { before, after })
}

/// The listing row for `key` shows a whole second inside that listing's own window.
fn listed_in_window(what: &str, listing: &Response, around: Around, key: &str) -> u64 {
    let listed = listed_millis(&listing.text(), key);
    assert_eq!(
        listed % SECOND,
        0,
        "{what}: the listing shows whole seconds"
    );
    assert_in_window(what, listed / SECOND, around, listing.date_secs(what))
}

/// Every leg-(7) check that holds both before and after the overwrite of `ahead`. Returns what
/// HEAD showed for `ahead`.
async fn check_wire(addr: SocketAddr, when: &str, behind_second: u64, probe: &str) -> u64 {
    // `ahead`: its stored time is in the future, so every surface shows the serving clock.
    let (head_shown, _) = shown_in_window(
        addr,
        &format!("{when}: HEAD {AHEAD}"),
        "HEAD",
        AHEAD,
        &[],
        200,
    )
    .await;
    shown_in_window(
        addr,
        &format!("{when}: GET {AHEAD}"),
        "GET",
        AHEAD,
        &[],
        200,
    )
    .await;
    // The ranged arm resolves through `get_object_range` and reads its clock on its own path,
    // so a `206` is pinned separately from the `200`.
    let range = ("range", "bytes=0-3");
    let (_, partial) = shown_in_window(
        addr,
        &format!("{when}: ranged GET {AHEAD}"),
        "GET",
        AHEAD,
        &[range],
        206,
    )
    .await;
    assert_eq!(
        partial.header("content-range"),
        Some(format!("bytes 0-3/{}", OLD.len()).as_str()),
        "{when}: ranged GET {AHEAD}"
    );
    let (listing, around) = list(addr, when).await;
    listed_in_window(&format!("{when}: listed {AHEAD}"), &listing, around, AHEAD);
    assert_eq!(
        listed_millis(&listing.text(), BEHIND),
        behind_second * SECOND,
        "{when}: a stored time in the past is listed exactly as stored"
    );
    // An instant after the test's start but before the stored time: the shown time is not
    // after it, so `If-Unmodified-Since` passes and `If-Modified-Since` answers `304`, on the
    // unranged and the ranged arm alike.
    let ius = send(
        addr,
        "HEAD",
        &object_path(AHEAD),
        "",
        &[("if-unmodified-since", probe)],
        b"",
    )
    .await;
    assert_eq!(
        ius.status, 200,
        "{when}: If-Unmodified-Since {probe} is judged against the shown time, so it holds \
         (not 412)"
    );
    let ranged_ius = send(
        addr,
        "GET",
        &object_path(AHEAD),
        "",
        &[("if-unmodified-since", probe), range],
        b"",
    )
    .await;
    assert_eq!(
        ranged_ius.status, 206,
        "{when}: ranged GET {AHEAD} If-Unmodified-Since {probe} is judged against the shown \
         time, so it holds (not 412)"
    );
    shown_in_window(
        addr,
        &format!("{when}: 304 for GET {AHEAD} If-Modified-Since {probe}"),
        "GET",
        AHEAD,
        &[("if-modified-since", probe)],
        304,
    )
    .await;
    shown_in_window(
        addr,
        &format!("{when}: 304 for ranged GET {AHEAD} If-Modified-Since {probe}"),
        "GET",
        AHEAD,
        &[("if-modified-since", probe), range],
        304,
    )
    .await;

    // `behind`: its stored time is in the past, so it is shown exactly as stored.
    let what = format!("{when}: HEAD {BEHIND}");
    let head = send(addr, "HEAD", &object_path(BEHIND), "", &[], b"").await;
    assert_eq!(head.status, 200, "{what}");
    assert_eq!(
        head.last_modified_secs(&what),
        behind_second,
        "{what}: the stored time, not the clock"
    );
    let at = http_date(behind_second);
    let what = format!("{when}: GET {BEHIND} If-Modified-Since {at}");
    let not_modified = send(
        addr,
        "GET",
        &object_path(BEHIND),
        "",
        &[("if-modified-since", &at)],
        b"",
    )
    .await;
    assert_eq!(not_modified.status, 304, "{what}");
    assert_eq!(
        not_modified.last_modified_secs(&what),
        behind_second,
        "{what}"
    );
    let earlier = http_date(behind_second - 1);
    let modified = send(
        addr,
        "GET",
        &object_path(BEHIND),
        "",
        &[("if-modified-since", &earlier)],
        b"",
    )
    .await;
    assert_eq!(
        modified.status, 200,
        "{when}: GET {BEHIND} If-Modified-Since {earlier}"
    );
    head_shown
}

/// (7) Over the real S3 wire: a future stored time is shown as the response time, a past one as
/// stored, on every surface and in both date preconditions, before and after an overwrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_shows_a_future_stored_time_as_the_response_time_and_a_past_one_as_stored() {
    let start = wall_millis();
    let ahead = mid_second(start + HOUR);
    let behind = mid_second(start - HOUR);
    let behind_second = behind / SECOND;
    let probe = http_date((start + 59 * MINUTE) / SECOND);
    let server = start_s3(&[(AHEAD, ahead), (BEHIND, behind)]).await;
    let addr = server.addr;

    let shown_before = check_wire(addr, "before the overwrite", behind_second, &probe).await;

    assert_eq!(OLD.len(), NEW.len(), "a same-size overwrite");
    let put = send(addr, "PUT", &object_path(AHEAD), "", &[], NEW).await;
    assert_eq!(put.status, 200, "the overwrite PUT: {}", put.text());

    let shown_after = check_wire(addr, "after the overwrite", behind_second, &probe).await;

    let what = "after the overwrite: HEAD ahead";
    let head = send(addr, "HEAD", &object_path(AHEAD), "", &[], b"").await;
    assert_eq!(
        head.header("etag"),
        Some(format!("\"{}\"", sha256_hex(NEW)).as_str()),
        "{what}: the new content's ETag"
    );
    assert!(
        shown_after >= shown_before,
        "{what}: the shown time {shown_after} must not be earlier than the {shown_before} \
         shown before the overwrite"
    );
    assert!(
        head.last_modified_secs(what) < ahead / SECOND,
        "{what}: the overwrite is shown earlier than the seed: a mirror that copied the file \
         while a gateway showed the seed holds a later time than this overwrite shows (the \
         documented exception)"
    );
}

/// (8) One healthy gateway, back-to-back same-size PUTs: no visible lead, never backwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wire_back_to_back_overwrites_on_one_healthy_gateway_show_no_lead() {
    let server = start_s3(&[]).await;
    let addr = server.addr;
    let key = "busy";
    let (mut last_head, mut last_listed) = (0, 0);
    for (step, body) in [OLD, NEW, NEWER, FOURTH].into_iter().enumerate() {
        assert_eq!(body.len(), OLD.len(), "same-size PUTs");
        let what = if step == 0 {
            "the create".to_owned()
        } else {
            format!("overwrite {step}")
        };
        let before_put = wall_secs();
        let put = send(addr, "PUT", &object_path(key), "", &[], body).await;
        assert_eq!(put.status, 200, "{what}: PUT {}", put.text());

        let head = send(addr, "HEAD", &object_path(key), "", &[], b"").await;
        let after_head = wall_secs();
        assert_eq!(head.status, 200, "{what}: HEAD");
        let head_what = format!("{what}: HEAD");
        let shown = assert_in_window(
            &head_what,
            head.last_modified_secs(&head_what),
            Around {
                before: before_put,
                after: after_head,
            },
            head.date_secs(&head_what),
        );
        assert!(
            shown >= last_head,
            "{what}: HEAD shows {shown}, earlier than the {last_head} it showed before"
        );
        last_head = shown;

        let listing = send(addr, "GET", &format!("/{BUCKET}"), "list-type=2", &[], b"").await;
        let after_listing = wall_secs();
        assert_eq!(listing.status, 200, "{what}: listing");
        let listed = listed_in_window(
            &format!("{what}: listed"),
            &listing,
            Around {
                before: before_put,
                after: after_listing,
            },
            key,
        );
        assert!(
            listed >= last_listed,
            "{what}: the listing shows {listed}, earlier than the {last_listed} it showed before"
        );
        last_listed = listed;
    }
}

#[test]
fn the_sdk_date_helpers_agree_on_a_known_instant() {
    // The wire legs format and parse dates with the SDK's own code, so their oracle is not the
    // gateway's formatter. Pin both directions on one independent vector.
    assert_eq!(http_date(1_700_000_000), "Tue, 14 Nov 2023 22:13:20 GMT");
    assert_eq!(
        http_date_secs("Tue, 14 Nov 2023 22:13:20 GMT", "vector"),
        1_700_000_000
    );
    assert_eq!(
        listed_millis(
            "<Contents><Key>k</Key><LastModified>2023-11-14T22:13:20.000Z</LastModified>\
             </Contents>",
            "k"
        ),
        1_700_000_000_000
    );
    assert_eq!(mid_second(1_700_000_000_999), 1_700_000_000_500);
}
