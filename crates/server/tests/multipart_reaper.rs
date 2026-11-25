//! The multipart **reaper**, driven through its operator one-shot (#625, slice 625.1; proposal
//! 0016 decision 6, `0016:1796-2277`).
//!
//! Every leg seeds a real redb store with raw session records, drops it (redb holds an exclusive
//! lock), runs the built binary as a subprocess —
//! `wyrd custodian --data-dir <dir> --reap-abandoned-uploads`, the flag last — waits for it with a
//! bound, reopens the store and asserts on what it durably holds. The command is stringly typed,
//! so this file names no symbol the reaper adds and compiles the same on a tree without it.
//!
//! * **A** — idle abandonment is reclaimed, every byte evidenced: an `Open` session that only the
//!   idle arm can reap (older than `W_open`, inside `W_session`, every lease lapsed) is fenced and
//!   drained, and an `orphan:` mark stands on every fragment position of its committed part and of
//!   its crashed in-flight residue.
//! * **B** — the idle arm never reaps a progressing upload: a live owned `sidx:` lease, a reserved
//!   slot with a renewed lease and no chunk yet, a recent part commit, and a recent reservation
//!   whose lease lapsed each keep the session.
//! * **C** — the residency ceiling still bites a session with fresh progress.
//! * **E-open** — the clock guard suppresses judgments, not the pass: foreign-clocked `Open`
//!   sessions past either window survive; a foreign-clocked `Aborting` session is still drained
//!   and deleted.
//! * **H** — a budget-profile mismatch stops the pass before it acts: the operator signal, and
//!   not one byte of the store changed, an already-`Aborting` upload's drain included. So does a
//!   lost admission ledger beside an upload, whose profile is then unknown.
//! * A session the pass cannot judge is held: the one-shot exits non-zero, and the pass still
//!   tears down the upload beside it.
//! * With no fleet, a record that could only be retired by listing the fleet is held, byte for
//!   byte, and the one-shot exits non-zero.
//! * Re-running the one-shot until it reports satisfied ends, for a store holding exactly one
//!   pass's budget of uploads and for one holding one more.
//! * The usage text names the one-shot and what it needs.
//! * A flag the one-shot would have to ignore — the fleet's three, or the GC sweep's arming flag —
//!   refuses it before it opens the store.
//! * A `--data-dir` holding no store is refused, and none is created: the one-shot never reports
//!   on a store it has just made. Its outcome line names the store it ran on.
//! * A zero-length `meta.redb` is no store either: refused, and left at zero bytes.
//!
//! Each runs plain `#[test]`s with short-lived runtimes of their own: the role `block_on`s its own.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use wyrd_core::metadata::{self, decode_orphan_mark, orphan_key, ChunkRef, EcScheme};
use wyrd_core::multipart::{
    decode_admission_record, mpu_key, part_key, part_range, psum_key, psum_range, retire_key,
    retire_session_range, sidx_key, sidx_range, slot_key, slot_range, AdmissionRecord, OwnedEntry,
    PartNumber, PartRecord, PartSummary, RetireMode, SessionRecord, SlotIndex, SlotRecord,
    StagedPlacement, UploadId, MAX_INFLIGHT_PARTS, MAX_PARTS_PER_SESSION, MAX_PART_CHUNKS,
    MAX_STAGED_CHUNKS, MPUCTL_KEY, W_REF,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, WriteBatch};

const WYRD: &str = env!("CARGO_BIN_EXE_wyrd");

/// The longest one one-shot may take. It runs one pass over a handful of records; a minute is
/// generous on a loaded CI host, and a hang past it fails the leg rather than the suite.
const ONE_SHOT_BOUND: Duration = Duration::from_secs(120);

/// How many one-shots leg A, C and E-open allow the store to converge in. A fence and the drain's
/// marks land in the first; the terminal delete waits for the `retire:` obligation to drain, so
/// it lands in the second or third. Six leaves room without hiding a pass that never converges.
const CONVERGE_RUNS: usize = 6;

const DAY: u64 = 24 * 60 * 60 * 1_000;
const HOUR: u64 = 60 * 60 * 1_000;

/// The deployment wall clock, read once per leg at seeding. The reaper judges every session
/// against the same source (`clock_source: "wall"`), so the fixture's stamps are written relative
/// to it.
// wall-clock exempt: the reaper under test reads the deployment wall clock, so its fixture's
// stamps must come from that same clock (#619).
#[allow(clippy::disallowed_methods)]
fn wall_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as u64
}

// ───────────────────────────── the store, outside the process ─────────────────────────────

/// A redb store under a temp dir the one-shot is pointed at.
struct Store {
    dir: tempfile::TempDir,
}

impl Store {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("temp dir"),
        }
    }

    fn data_dir(&self) -> &Path {
        self.dir.path()
    }

    /// Run `f` over the store, opened for the call and closed before it returns, in a runtime of
    /// its own.
    fn with<T>(&self, f: impl AsyncFnOnce(&RedbMetadataStore) -> T) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let meta = RedbMetadataStore::open(self.data_dir().join("meta.redb")).expect("redb");
            f(&meta).await
        })
    }

    /// Put every record, in one batch.
    fn seed(&self, records: Vec<(Vec<u8>, Bytes)>) {
        self.with(async |meta| {
            let batch = records
                .into_iter()
                .fold(WriteBatch::new(), |batch, (key, value)| {
                    batch.put(key, value)
                });
            let outcome = meta.commit(batch).await.expect("seeded");
            assert_eq!(outcome, CommitOutcome::Committed);
        });
    }

    /// Every key and value the store holds.
    fn snapshot(&self) -> BTreeMap<Vec<u8>, Bytes> {
        self.with(async |meta| {
            meta.scan(b"")
                .await
                .expect("scan")
                .into_iter()
                .collect::<BTreeMap<_, _>>()
        })
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        let key = key.to_vec();
        self.with(async move |meta| meta.get(&key).await.expect("get"))
    }

    fn range(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Bytes)> {
        let prefix = prefix.to_vec();
        self.with(async move |meta| meta.scan(&prefix).await.expect("scan"))
    }

    /// The `mpuctl` count.
    fn counted(&self) -> u64 {
        let ledger = self.value(MPUCTL_KEY).expect("the admission ledger");
        decode_admission_record(&ledger)
            .expect("the ledger decodes")
            .count()
    }

    /// One one-shot over this store: its exit status and what it wrote to stderr.
    fn reap_once(&self) -> (ExitStatus, String) {
        reap_once_at(self.data_dir(), &[])
    }

    /// The `[store: …]` tail of a one-shot's outcome line over this store.
    fn named(&self) -> String {
        format!(
            "[store: redb at {}]",
            self.data_dir().join("meta.redb").display()
        )
    }

    /// One-shots until `done` holds, at most [`CONVERGE_RUNS`]. The store's convergence is
    /// asserted first, then that every one-shot exited successfully: a leg fails on what the store
    /// holds before it fails on a status.
    fn reap_until(&self, done: impl Fn(&Self) -> bool) {
        let mut failed = Vec::new();
        for run in 0..CONVERGE_RUNS {
            if done(self) {
                break;
            }
            let (status, stderr) = self.reap_once();
            if !status.success() {
                failed.push(format!("one-shot {run} exited {status}:\n{stderr}"));
            }
        }
        assert!(
            done(self),
            "the store did not converge within {CONVERGE_RUNS} one-shots\n{}",
            failed.join("\n")
        );
        assert!(failed.is_empty(), "{}", failed.join("\n"));
    }
}

/// One one-shot pointed at `data_dir`, with `extra` arguments before the one-shot's own flag: its
/// exit status and what it wrote to stderr.
fn reap_once_at(data_dir: &Path, extra: &[&str]) -> (ExitStatus, String) {
    let mut command = Command::new(WYRD);
    command
        .arg("custodian")
        .arg("--data-dir")
        .arg(data_dir)
        .args(extra)
        // Last in argv: on a parser that does not know it as valueless, the command fails at
        // parse and reaps nothing.
        .arg("--reap-abandoned-uploads");
    run_bounded(command)
}

/// Run `command` to its end, within [`ONE_SHOT_BOUND`]: its exit status and what it wrote to
/// stderr.
fn run_bounded(mut command: Command) -> (ExitStatus, String) {
    let mut child = Reaped(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn wyrd"),
    );
    // Read while the child runs: a child whose stderr fills the pipe must never block on it.
    let mut pipe = child.0.stderr.take().expect("stderr");
    let drained = std::thread::spawn(move || {
        let mut stderr = String::new();
        pipe.read_to_string(&mut stderr).map(|_| stderr)
    });
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("wait") {
            break status;
        }
        assert!(
            started.elapsed() < ONE_SHOT_BOUND,
            "the command must return after one pass, within {ONE_SHOT_BOUND:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let stderr = drained
        .join()
        .expect("the stderr reader")
        .expect("stderr is utf-8");
    (status, stderr)
}

/// The child process, killed on every exit path so a hang never outlives the test.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ─────────────────────────────── the records ───────────────────────────────

/// Decode `json` as `T` and re-encode it: the canonical bytes the store's decoders require.
fn canonical<T: serde::Serialize + serde::de::DeserializeOwned>(json: &str) -> Bytes {
    let value: T = metadata::decode(json.as_bytes()).expect("a legal record");
    metadata::encode(&value)
}

fn upload(n: u8) -> UploadId {
    UploadId::new(format!("{n:02x}").repeat(16)).expect("an upload id")
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number")
}

const RS21: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };

/// One RS(2,1) chunk placed on `servers`.
fn chunk(id: ChunkId, servers: [DServerId; 3]) -> ChunkRef {
    ChunkRef {
        id,
        scheme: RS21,
        len: 4_096,
        placement: servers.to_vec(),
    }
}

/// Every fragment position of `chunk`: fragment `i` on `placement[i]`.
fn positions(chunk: &ChunkRef) -> Vec<(DServerId, FragmentId)> {
    chunk
        .placement
        .iter()
        .enumerate()
        .map(|(i, &dserver)| {
            (
                dserver,
                FragmentId {
                    chunk: chunk.id,
                    index: i as u16,
                },
            )
        })
        .collect()
}

/// The shipped budget profile, spelled from its public constants: the one the custodian under
/// test holds as its own.
fn shipped_profile_json(w_ref: u64) -> String {
    format!(
        "{{\"w_ref\":{w_ref},\"max_part_chunks\":{MAX_PART_CHUNKS},\
         \"max_parts_per_session\":{MAX_PARTS_PER_SESSION},\
         \"max_inflight_parts\":{MAX_INFLIGHT_PARTS},\"max_staged_chunks\":{MAX_STAGED_CHUNKS}}}"
    )
}

/// `mpuctl` counting `count` sessions under the profile with `W_ref = w_ref` (and every other
/// number shipped), its `max_sessions` the profile's own derivation.
fn ledger(count: u64, w_ref: u64) -> (Vec<u8>, Bytes) {
    let profile = shipped_profile_json(w_ref);
    let profile_budget: wyrd_core::multipart::Budget =
        metadata::decode(profile.as_bytes()).expect("a legal profile");
    let json = format!(
        "{{\"count\":{count},\"max_sessions\":{},\"profile\":{profile}}}",
        profile_budget.max_sessions()
    );
    (MPUCTL_KEY.to_vec(), canonical::<AdmissionRecord>(&json))
}

/// The session record of `id` in `state` (its JSON), created at `created`, stamped under `clock`.
fn session_json(id: &UploadId, created: u64, clock: &str, epoch: u64, state: &str) -> String {
    format!(
        "{{\"parent\":42,\"object\":\"bucket/object-{id}\",\"created_at_millis\":{created},\
         \"clock_source\":\"{clock}\",\"segment_nonce\":\"{}\",\"epoch\":{epoch},\"attempts\":0,\
         \"state\":{{\"kind\":\"{state}\"}}}}",
        id.as_str()
    )
}

/// An `Open@0` session record.
fn open_session(id: &UploadId, created: u64, clock: &str) -> (Vec<u8>, Bytes) {
    let json = session_json(id, created, clock, 0, "Open");
    (mpu_key(id), canonical::<SessionRecord>(&json))
}

/// A committed part `n` of `id` over `chunks`, committed at `at`: its `part:` and `psum:` records.
fn committed_part(id: &UploadId, n: u32, chunks: &[ChunkRef], at: u64) -> Vec<(Vec<u8>, Bytes)> {
    let digest = format!("{:064x}", n);
    let len: u64 = chunks.iter().map(|c| c.len).sum();
    let chunks_json = String::from_utf8(metadata::encode(&chunks).to_vec()).unwrap();
    let part_json = format!(
        "{{\"chunks\":{chunks_json},\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{at},\"session_epoch\":0}}"
    );
    let summary_json = format!(
        "{{\"chunks\":{},\"len\":{len},\"digest\":\"{digest}\",\"committed_at_millis\":{at}}}",
        chunks.len()
    );
    vec![
        (part_key(id, part(n)), canonical::<PartRecord>(&part_json)),
        (
            psum_key(id, part(n)),
            canonical::<PartSummary>(&summary_json),
        ),
    ]
}

/// Slot `index` of `id`, reserved for part `n` at `reserved`, leased to `lease`.
fn slot(id: &UploadId, index: u32, n: u32, reserved: u64, lease: u64) -> (Vec<u8>, Bytes) {
    let json = format!(
        "{{\"part_number\":{n},\"attempt_id\":\"{}\",\"reserved_at_millis\":{reserved},\
         \"lease_expiry_millis\":{lease}}}",
        "ab".repeat(16)
    );
    (
        slot_key(id, SlotIndex::new(index).expect("a slot index")),
        canonical::<SlotRecord>(&json),
    )
}

/// The owned staging entry of `chunk` for part `n` of `id`, leased to `lease`.
fn owned(id: &UploadId, n: u32, chunk: &ChunkRef, lease: u64) -> (Vec<u8>, Bytes) {
    let staged = StagedPlacement::new(chunk.scheme, chunk.placement.clone()).expect("staged");
    let entry = OwnedEntry::new(id.clone(), lease, staged).to_pending();
    (sidx_key(id, part(n), chunk.id), metadata::encode(&entry))
}

/// An upload already `Aborting` under `clock`: fenced from `Open@0` by the one teardown the client
/// Abort and the reaper share, its `{session, all}` obligation beside it, holding one committed
/// part over `committed` and the residue of a crashed in-flight part over `residue`.
fn aborting_session(
    id: &UploadId,
    clock: &str,
    now: u64,
    committed: &ChunkRef,
    residue: &ChunkRef,
) -> Vec<(Vec<u8>, Bytes)> {
    let mut records = fenced(id, clock, now);
    records.extend([
        slot(id, 0, 2, now - 10 * DAY, now - 9 * DAY),
        owned(id, 2, residue, now - 9 * DAY),
    ]);
    records.extend(committed_part(
        id,
        1,
        std::slice::from_ref(committed),
        now - 10 * DAY,
    ));
    records
}

/// The session record of `id` at `Aborting` under `clock`, and its `{session, all}` obligation:
/// what the one teardown fence leaves of an `Open@0` session created 14 days before `now`.
fn fenced(id: &UploadId, clock: &str, now: u64) -> Vec<(Vec<u8>, Bytes)> {
    let open: SessionRecord =
        metadata::decode(session_json(id, now - 14 * DAY, clock, 0, "Open").as_bytes())
            .expect("a session");
    let teardown = open.open_teardown(id).expect("Open fences");
    let obligation = teardown.obligation();
    vec![
        (mpu_key(id), metadata::encode(teardown.session())),
        (obligation.key(), metadata::encode(obligation.payload())),
    ]
}

/// Which outcome a one-shot reported on stderr: the word after `multipart reaper pass`.
fn reported(stderr: &str) -> String {
    stderr
        .split("multipart reaper pass ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or("nothing")
        .to_owned()
}

/// The reaper tally `name` a one-shot emitted: the number that follows
/// `monotonic_counter.<name>` on its line, however the log format separates the two.
fn tally(stderr: &str, name: &str) -> Option<u64> {
    let line = stderr
        .lines()
        .find_map(|line| line.split(&format!("monotonic_counter.{name}")).nth(1))?;
    let digits: String = line
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// Whether every staged and session record of `id` is gone.
fn session_gone(store: &Store, id: &UploadId) -> bool {
    store.value(&mpu_key(id)).is_none()
        && [
            part_range(id),
            psum_range(id),
            sidx_range(id),
            slot_range(id),
        ]
        .iter()
        .all(|prefix| store.range(prefix).is_empty())
}

/// Assert the whole teardown of `id`: every record gone, both of its `retire:` ranges empty.
fn assert_torn_down(store: &Store, id: &UploadId) {
    assert!(store.value(&mpu_key(id)).is_none(), "mpu:{id} deleted");
    for (name, prefix) in [
        ("part:", part_range(id)),
        ("psum:", psum_range(id)),
        ("sidx:", sidx_range(id)),
        ("slot:", slot_range(id)),
        ("retire:bytes:", retire_session_range(RetireMode::Bytes, id)),
        (
            "retire:records:",
            retire_session_range(RetireMode::Records, id),
        ),
    ] {
        assert!(store.range(&prefix).is_empty(), "{name}{id} is empty");
    }
}

/// Assert GC's licence for every position of `expected`: an `orphan:` mark stands there, it
/// decodes stamped (not `reclaiming`), the `retire:bytes:` obligation its event names (if any) is
/// gone, and no `part:` or `sidx:` record in the store still names the position.
fn assert_licensed(store: &Store, expected: &[(DServerId, FragmentId)]) {
    let naming: Vec<(DServerId, FragmentId)> = store
        .range(b"part:")
        .iter()
        .flat_map(|(_, value)| {
            let part: PartRecord = metadata::decode(value).expect("a part");
            part.chunks().iter().flat_map(positions).collect::<Vec<_>>()
        })
        .chain(store.range(b"sidx:").iter().flat_map(|(key, value)| {
            let (_, chunk_id, entry) =
                wyrd_core::multipart::decode_owned_entry(key, value).expect("an owned entry");
            let staged = entry.staged();
            positions(&ChunkRef {
                id: chunk_id,
                scheme: staged.scheme(),
                len: 0,
                placement: staged.placement().to_vec(),
            })
        }))
        .collect();
    for &(dserver, frag) in expected {
        let key = orphan_key(dserver, frag);
        let value = store
            .value(&key)
            .unwrap_or_else(|| panic!("an orphan: mark on server {dserver}, {frag:?}"));
        let mark = decode_orphan_mark(&value).expect("the mark decodes");
        assert!(!mark.is_reclaiming(), "{frag:?} is stamped, not reclaiming");
        if let Some(token) = mark.retire_token() {
            assert!(
                store
                    .value(&retire_key(RetireMode::Bytes, &token))
                    .is_none(),
                "the obligation {token} the mark names has drained"
            );
        }
        assert!(
            !naming.contains(&(dserver, frag)),
            "no record still names {dserver}/{frag:?}"
        );
    }
}

// ─────────────────────────────── the legs ───────────────────────────────

/// **A — idle abandonment is reclaimed, with every byte evidenced.** Only the idle arm can reap
/// this session: it was created 14 days ago (past `W_open`, inside `W_session`), its newest
/// progress stamp is 10 days old, and every lease lapsed 9 days ago. It holds one committed part
/// and the residue of a crashed in-flight part (a slot and an owned entry, no part record).
#[test]
fn idle_abandoned_session_is_fenced_and_drained_with_every_byte_evidenced() {
    let store = Store::new();
    let now = wall_now();
    let id = upload(0xa1);
    let committed = chunk(0xa1_0001, [1, 2, 3]);
    let in_flight = chunk(0xa1_0002, [4, 5, 6]);
    let mut records = vec![
        ledger(1, W_REF),
        open_session(&id, now - 14 * DAY, "wall"),
        slot(&id, 0, 2, now - 10 * DAY, now - 9 * DAY),
        owned(&id, 2, &in_flight, now - 9 * DAY),
    ];
    records.extend(committed_part(
        &id,
        1,
        std::slice::from_ref(&committed),
        now - 10 * DAY,
    ));
    store.seed(records);
    assert_eq!(store.counted(), 1);

    store.reap_until(|store| session_gone(store, &id));

    assert_torn_down(&store, &id);
    assert_eq!(store.counted(), 0, "the admission slot is released");
    // Both byte sets: the committed part's positions, named explicitly, and the residue's.
    let committed_positions = positions(&committed);
    assert_eq!(
        committed_positions,
        vec![
            (
                1,
                FragmentId {
                    chunk: 0xa1_0001,
                    index: 0
                }
            ),
            (
                2,
                FragmentId {
                    chunk: 0xa1_0001,
                    index: 1
                }
            ),
            (
                3,
                FragmentId {
                    chunk: 0xa1_0001,
                    index: 2
                }
            ),
        ]
    );
    assert_licensed(&store, &committed_positions);
    assert_licensed(&store, &positions(&in_flight));
}

/// **B — the idle arm never reaps a progressing upload.** Four sessions, each created 14 days
/// ago (inside `W_session`) with a part committed 10 days ago, each with one piece of evidence
/// of progress: (i) an owned `sidx:` lease still unexpired (one long `UploadPart` streaming),
/// (ii) a reserved slot with no chunk written yet and a renewed lease, (iii) a part committed an
/// hour ago, (iv) a slot reserved an hour ago whose lease has already lapsed — the reservation
/// stamp alone is progress. Two one-shots leave every byte of the store as it was, the admission
/// count with it.
#[test]
fn idle_arm_never_reaps_a_progressing_upload() {
    let store = Store::new();
    let now = wall_now();
    let created = now - 14 * DAY;
    let old = now - 10 * DAY;
    let (streaming, pre_first_chunk, recent, reserved) =
        (upload(0xb1), upload(0xb2), upload(0xb3), upload(0xb4));
    let mut records = vec![
        ledger(4, W_REF),
        open_session(&streaming, created, "wall"),
        open_session(&pre_first_chunk, created, "wall"),
        open_session(&recent, created, "wall"),
        open_session(&reserved, created, "wall"),
        // (i) a live owned lease, renewed an hour out.
        owned(&streaming, 2, &chunk(0xb1_0002, [4, 5, 6]), now + HOUR),
        // (ii) reserved 10 days ago, no chunk yet, its lease renewed an hour out.
        slot(&pre_first_chunk, 0, 2, old, now + HOUR),
        // (iv) reserved an hour ago, its lease lapsed half an hour ago.
        slot(&reserved, 0, 2, now - HOUR, now - HOUR / 2),
    ];
    for (n, id) in [(1, &streaming), (2, &pre_first_chunk), (4, &reserved)] {
        records.extend(committed_part(
            id,
            1,
            &[chunk(0xb0_0000 + n, [1, 2, 3])],
            old,
        ));
    }
    // (iii) the newest part committed an hour ago.
    records.extend(committed_part(
        &recent,
        1,
        &[chunk(0xb3_0001, [1, 2, 3])],
        old,
    ));
    records.extend(committed_part(
        &recent,
        2,
        &[chunk(0xb3_0002, [1, 2, 3])],
        now - HOUR,
    ));
    store.seed(records);
    let before = store.snapshot();

    let runs: Vec<(ExitStatus, String)> = (0..2).map(|_| store.reap_once()).collect();

    assert_eq!(
        store.snapshot(),
        before,
        "no record of a progressing upload moved"
    );
    assert_eq!(store.counted(), 4, "the admission count is unchanged");
    // And the one-shot ran its pass: a store left alone by a command that never ran proves
    // nothing about the reaper. Its own tallies say it judged the four uploads and kept each;
    // it read the listing whole and found nothing to end — on this store, which its outcome line
    // names.
    for (run, (status, stderr)) in runs.iter().enumerate() {
        assert!(
            status.success(),
            "one-shot {run} failed: {status}\n{stderr}"
        );
        for (name, expected) in [
            ("reaper_passes", 1),
            ("reaper_sessions_kept", 4),
            ("reaper_sessions_fenced_idle", 0),
            ("reaper_sessions_fenced_ceiling", 0),
        ] {
            assert_eq!(
                tally(stderr, name),
                Some(expected),
                "one-shot {run}: the reaper's {name}:\n{stderr}"
            );
        }
        assert_eq!(reported(stderr), "satisfied", "one-shot {run}:\n{stderr}");
        let outcome = stderr
            .lines()
            .find(|line| line.contains("multipart reaper pass "))
            .unwrap_or_else(|| panic!("one-shot {run} reports its pass:\n{stderr}"));
        assert!(
            outcome.ends_with(&store.named()),
            "one-shot {run} names the store it ran on: {outcome}"
        );
    }
}

/// **C — the residency ceiling still bites.** A session created 40 days ago (past `W_session`)
/// with fresh progress everywhere — a part committed an hour ago, a live slot, a live owned lease
/// — is fenced and drained all the same, and its admission slot released.
#[test]
fn over_age_session_is_fenced_and_drained_however_live() {
    let store = Store::new();
    let now = wall_now();
    let id = upload(0xc1);
    let committed = chunk(0xc1_0001, [1, 2, 3]);
    let in_flight = chunk(0xc1_0002, [4, 5, 6]);
    let mut records = vec![
        ledger(1, W_REF),
        open_session(&id, now - 40 * DAY, "wall"),
        slot(&id, 0, 2, now - HOUR, now + HOUR),
        owned(&id, 2, &in_flight, now + HOUR),
    ];
    records.extend(committed_part(
        &id,
        1,
        std::slice::from_ref(&committed),
        now - HOUR,
    ));
    store.seed(records);

    store.reap_until(|store| session_gone(store, &id));

    assert_torn_down(&store, &id);
    assert_eq!(store.counted(), 0, "the admission slot is released");
    assert_licensed(&store, &positions(&committed));
    assert_licensed(&store, &positions(&in_flight));
}

/// **E-open — the clock guard suppresses judgments, not the pass.** Two `Open` sessions stamped
/// under a clock the reaper does not read — one past `W_open` only, one past `W_session` — are
/// never judged and survive byte for byte. Only an operator can end them, so every one-shot
/// names them and exits non-zero: it never reports such a store as having nothing to reap. A
/// third, already `Aborting` under the same foreign clock with its obligation installed, is
/// walked, drained and deleted all the same, which reads no session timestamp at all.
#[test]
fn foreign_clocked_open_sessions_survive_and_a_foreign_aborting_one_drains() {
    let store = Store::new();
    let now = wall_now();
    let (idle, over_age, aborting) = (upload(0xe1), upload(0xe2), upload(0xe3));
    let mut records = vec![
        ledger(3, W_REF),
        open_session(&idle, now - 14 * DAY, "logical"),
        slot(&idle, 0, 2, now - 10 * DAY, now - 9 * DAY),
        open_session(&over_age, now - 40 * DAY, "logical"),
    ];
    records.extend(committed_part(
        &idle,
        1,
        &[chunk(0xe1_0001, [1, 2, 3])],
        now - 10 * DAY,
    ));
    records.extend(committed_part(
        &over_age,
        1,
        &[chunk(0xe2_0001, [1, 2, 3])],
        now - 35 * DAY,
    ));
    let aborted_part = chunk(0xe3_0001, [1, 2, 3]);
    let aborted_residue = chunk(0xe3_0002, [4, 5, 6]);
    records.extend(aborting_session(
        &aborting,
        "logical",
        now,
        &aborted_part,
        &aborted_residue,
    ));
    store.seed(records);
    let survivors = |snapshot: &BTreeMap<Vec<u8>, Bytes>| -> BTreeMap<Vec<u8>, Bytes> {
        snapshot
            .iter()
            .filter(|(key, _)| {
                [&idle, &over_age]
                    .iter()
                    .any(|id| key.windows(32).any(|w| w == id.as_str().as_bytes()))
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    };
    let before = survivors(&store.snapshot());
    assert!(before.len() >= 6, "the foreign Open sessions are seeded");

    let mut runs = Vec::new();
    for _ in 0..CONVERGE_RUNS {
        if session_gone(&store, &aborting) {
            break;
        }
        runs.push(store.reap_once());
    }

    assert!(
        session_gone(&store, &aborting),
        "the foreign Aborting session is torn down within {CONVERGE_RUNS} one-shots"
    );
    assert_eq!(
        survivors(&store.snapshot()),
        before,
        "neither foreign-clocked Open session was judged"
    );
    assert_torn_down(&store, &aborting);
    assert_eq!(
        store.counted(),
        2,
        "the Aborting session's slot is released"
    );
    assert_licensed(&store, &positions(&aborted_part));
    assert_licensed(&store, &positions(&aborted_residue));
    for (run, (status, stderr)) in runs.iter().enumerate() {
        for id in [&idle, &over_age] {
            assert!(
                stderr.contains(&format!("mpu:{id}")) && stderr.contains("foreign-clock"),
                "one-shot {run} names {id} as held for its clock:\n{stderr}"
            );
        }
        assert!(
            !status.success(),
            "one-shot {run} exits non-zero while a foreign-clocked Open session waits for an \
             operator: {status}"
        );
    }
}

/// **H — a profile mismatch stops the reaper before it acts.** The ledger stores a profile with
/// twice the shipped `W_ref`, so the custodian is not sized for the sessions it would fence. A
/// session the idle arm would reap (leg A's) and one past the ceiling (leg C's) are both present,
/// and so is one already `Aborting` with its `retire:bytes:` obligation, a committed part and
/// in-flight residue — work the retirement drain would otherwise do this same pass. The one-shot
/// emits the operator signal and exits non-zero, and the store is byte-identical: no fence, no
/// mark, no deletion, no counter change.
#[test]
fn profile_mismatch_signals_and_mutates_nothing() {
    let store = Store::new();
    let now = wall_now();
    let (idle, over_age, aborting) = (upload(0x71), upload(0x72), upload(0x73));
    let mut records = vec![
        ledger(3, 2 * W_REF),
        open_session(&idle, now - 14 * DAY, "wall"),
        slot(&idle, 0, 2, now - 10 * DAY, now - 9 * DAY),
        owned(&idle, 2, &chunk(0x71_0002, [4, 5, 6]), now - 9 * DAY),
        open_session(&over_age, now - 40 * DAY, "wall"),
    ];
    records.extend(committed_part(
        &idle,
        1,
        &[chunk(0x71_0001, [1, 2, 3])],
        now - 10 * DAY,
    ));
    records.extend(aborting_session(
        &aborting,
        "wall",
        now,
        &chunk(0x73_0001, [1, 2, 3]),
        &chunk(0x73_0002, [4, 5, 6]),
    ));
    store.seed(records);
    let before = store.snapshot();

    let (status, stderr) = store.reap_once();

    assert!(
        stderr.contains("reaper-profile-mismatch"),
        "the operator signal names the mismatch:\n{stderr}"
    );
    assert!(
        stderr.contains("refused before acting"),
        "the signal says the pass acted on nothing:\n{stderr}"
    );
    assert!(!status.success(), "a refused pass exits non-zero: {status}");
    assert_eq!(store.snapshot(), before, "the store is untouched");
    assert_eq!(store.counted(), 3);
}

/// **H — a lost admission ledger stops the reaper and the drain before they act.** No `mpuctl`,
/// and beside it an upload already `Aborting` with its `retire:bytes:` obligation, a committed part
/// and in-flight residue, and an `Open` session the idle arm would reap. Every Create writes the
/// ledger with its session, so the profile they were admitted under is unknown: the one-shot emits
/// the operator signal and exits non-zero, and the store is byte-identical — no fence, no mark, no
/// deletion, no ledger written.
#[test]
fn lost_ledger_signals_and_mutates_nothing() {
    let store = Store::new();
    let now = wall_now();
    let (idle, aborting) = (upload(0x74), upload(0x75));
    let mut records = vec![
        open_session(&idle, now - 14 * DAY, "wall"),
        slot(&idle, 0, 2, now - 10 * DAY, now - 9 * DAY),
    ];
    records.extend(committed_part(
        &idle,
        1,
        &[chunk(0x74_0001, [1, 2, 3])],
        now - 10 * DAY,
    ));
    records.extend(aborting_session(
        &aborting,
        "wall",
        now,
        &chunk(0x75_0001, [1, 2, 3]),
        &chunk(0x75_0002, [4, 5, 6]),
    ));
    store.seed(records);
    let before = store.snapshot();
    assert!(!before.contains_key(MPUCTL_KEY));

    let (status, stderr) = store.reap_once();

    assert!(
        stderr.contains("reaper-profile-mismatch") && stderr.contains("mpuctl is absent"),
        "the operator signal names the lost ledger:\n{stderr}"
    );
    assert!(
        stderr.contains("refused before acting"),
        "the signal says the pass acted on nothing:\n{stderr}"
    );
    assert!(!status.success(), "a refused pass exits non-zero: {status}");
    assert_eq!(store.snapshot(), before, "the store is untouched");
}

/// **A held session fails the one-shot, and the pass goes on.** Beside leg A's idle session sits
/// a session record that does not decode. Every one-shot names it (`reaper-session-held`) and
/// exits non-zero, so a script running the one-shot sees that something needs a human — yet each
/// pass still fences, drains and deletes the idle session around it, and the undecodable record
/// is left byte for byte.
#[test]
fn a_held_session_fails_the_one_shot_and_the_pass_goes_on() {
    let store = Store::new();
    let now = wall_now();
    let (idle, damaged) = (upload(0x81), upload(0x82));
    let committed = chunk(0x81_0001, [1, 2, 3]);
    let mut records = vec![
        ledger(2, W_REF),
        open_session(&idle, now - 14 * DAY, "wall"),
        (
            mpu_key(&damaged),
            Bytes::from_static(b"not a session record"),
        ),
    ];
    records.extend(committed_part(
        &idle,
        1,
        std::slice::from_ref(&committed),
        now - 10 * DAY,
    ));
    store.seed(records);

    let mut runs = Vec::new();
    for _ in 0..CONVERGE_RUNS {
        if session_gone(&store, &idle) {
            break;
        }
        runs.push(store.reap_once());
    }

    assert!(
        session_gone(&store, &idle),
        "the idle session is torn down around the held one within {CONVERGE_RUNS} one-shots"
    );
    assert_torn_down(&store, &idle);
    assert_licensed(&store, &positions(&committed));
    assert_eq!(
        store.value(&mpu_key(&damaged)).as_deref(),
        Some(&b"not a session record"[..]),
        "the held record is left as it was"
    );
    assert_eq!(
        store.counted(),
        1,
        "only the idle session's slot is released"
    );
    for (run, (status, stderr)) in runs.iter().enumerate() {
        assert!(
            stderr.contains("reaper-session-held"),
            "one-shot {run} names the hold:\n{stderr}"
        );
        assert!(
            !status.success(),
            "one-shot {run} exits non-zero while a session is held: {status}"
        );
    }
}

/// **With no fleet, what only a fleet listing could retire is held, and the one-shot fails.** Two
/// uploads already `Aborting`, each with its `{session, all}` obligation: one holds an owned
/// entry that does not decode, the other a committed part naming two D servers for a scheme of
/// three fragments (two uploads, because an upload's obligation waits for its owned residue).
/// Neither record says where its bytes are, so retiring them means finding them by chunk id on
/// the whole fleet — and the one-shot lists no D server. Every one-shot names both as held for
/// the fleet and exits non-zero, and the store is byte-identical: no record is deleted on the
/// strength of a listing of nothing, and no mark is written.
#[test]
fn what_only_a_fleet_listing_could_retire_is_held_and_fails_the_one_shot() {
    let store = Store::new();
    let now = wall_now();
    let (residue, damaged) = (upload(0x91), upload(0x92));
    let undecodable = sidx_key(&residue, part(2), 0x91_0002);
    let short = ChunkRef {
        placement: vec![1, 2],
        ..chunk(0x92_0001, [1, 2, 3])
    };
    let mut records = vec![
        ledger(2, W_REF),
        (
            undecodable.clone(),
            Bytes::from_static(b"not an owned entry"),
        ),
    ];
    records.extend(fenced(&residue, "wall", now));
    records.extend(fenced(&damaged, "wall", now));
    records.extend(committed_part(&damaged, 1, &[short], now - 10 * DAY));
    store.seed(records);
    let before = store.snapshot();

    for run in 0..2 {
        let (status, stderr) = store.reap_once();

        let held = |action: &str, record: &[u8]| {
            let record = String::from_utf8_lossy(record).into_owned();
            stderr.lines().any(|line| {
                line.contains(action) && line.contains("partial-fleet") && line.contains(&record)
            })
        };
        assert!(
            held("retire-session-held", &undecodable),
            "one-shot {run} holds the owned entry for the fleet:\n{stderr}"
        );
        assert!(
            held("retire-obligation-held", &part_key(&damaged, part(1))),
            "one-shot {run} holds the part for the fleet:\n{stderr}"
        );
        assert_eq!(reported(&stderr), "BLOCKED", "one-shot {run}:\n{stderr}");
        assert!(
            !status.success(),
            "one-shot {run} exits non-zero while a record waits for a fleet pass: {status}"
        );
        assert_eq!(
            store.snapshot(),
            before,
            "one-shot {run} left the store untouched"
        );
    }
}

/// **Re-running the one-shot until it reports satisfied ends.** The reaper judges at most 64
/// uploads a pass (its `SESSIONS_PER_PASS`, spelled as a number because this file names no symbol
/// the reaper adds). A store holding exactly 64 uploads the reaper keeps — each created a day ago
/// — is judged whole by one one-shot, which reports satisfied: a full last page is not taken for
/// an unfinished listing. One holding 65 takes two: the first reports partial, the second ends
/// the lap and reports satisfied, and so on every lap after. No upload record moves, and the
/// admission count stays.
#[test]
fn rerunning_the_one_shot_until_satisfied_ends_at_and_past_the_pass_budget() {
    for (count, expected) in [
        (64_u8, &["satisfied", "satisfied"][..]),
        (65, &["partial", "satisfied", "partial", "satisfied"]),
    ] {
        let store = Store::new();
        let now = wall_now();
        let mut records = vec![ledger(u64::from(count), W_REF)];
        records.extend((0..count).map(|n| open_session(&upload(n), now - DAY, "wall")));
        store.seed(records);
        let uploads = store.range(b"mpu:");
        assert_eq!(uploads.len(), usize::from(count));

        let reports: Vec<String> = expected
            .iter()
            .enumerate()
            .map(|(run, _)| {
                let (status, stderr) = store.reap_once();
                assert!(
                    status.success(),
                    "{count} uploads, one-shot {run} failed: {status}\n{stderr}"
                );
                reported(&stderr)
            })
            .collect();

        assert_eq!(reports, expected, "{count} uploads");
        assert_eq!(store.range(b"mpu:"), uploads, "{count} uploads: none moved");
        assert_eq!(store.counted(), u64::from(count));
    }
}

/// **The usage text names the one-shot.** `wyrd` with no command prints its usage and exits 2:
/// the custodian synopsis lists `--reap-abandoned-uploads`, and its own line says what an operator
/// needs to know before running it — it touches metadata only, needs no `--endpoints`, and exits
/// non-zero when an upload is held or the profile check refuses.
#[test]
fn usage_names_the_one_shot_and_what_it_needs() {
    let (status, stderr) = run_bounded(Command::new(WYRD));

    assert_eq!(status.code(), Some(2), "{stderr}");
    let synopsis = stderr
        .lines()
        .find(|line| line.trim_start().starts_with("wyrd custodian "))
        .unwrap_or_else(|| panic!("a custodian synopsis:\n{stderr}"));
    assert!(
        synopsis.contains("[--reap-abandoned-uploads]"),
        "the synopsis lists the flag: {synopsis}"
    );
    let line = stderr
        .lines()
        .find(|line| line.trim_start().starts_with("--reap-abandoned-uploads "))
        .unwrap_or_else(|| panic!("a line describing the flag:\n{stderr}"));
    for needed in [
        "ONE multipart reaper pass",
        "metadata-only",
        "needs no --endpoints",
        "exits non-zero",
        "session is held",
        "profile check refuses",
    ] {
        assert!(line.contains(needed), "the line says `{needed}`: {line}");
    }
}

/// **A flag the one-shot would have to ignore refuses it.** The one-shot runs the reaper and its
/// record-only drain: no GC pass, and nothing that dials a D server. So `--gc-expired-pending`,
/// which arms a GC sweep, and `--endpoints`, `--ids` and `--failure-domains`, which describe a
/// fleet, each stop it before it opens the store — named in the message beside the one-shot's own
/// flag, a non-zero exit, no pass reported — and leg A's idle upload, there to be reaped, is left
/// byte for byte.
#[test]
fn a_flag_the_one_shot_would_ignore_refuses_it_before_it_acts() {
    let store = Store::new();
    let now = wall_now();
    let id = upload(0xf1);
    let mut records = vec![
        ledger(1, W_REF),
        open_session(&id, now - 14 * DAY, "wall"),
        slot(&id, 0, 2, now - 10 * DAY, now - 9 * DAY),
    ];
    records.extend(committed_part(
        &id,
        1,
        &[chunk(0xf1_0001, [1, 2, 3])],
        now - 10 * DAY,
    ));
    store.seed(records);
    let before = store.snapshot();

    for (flag, extra) in [
        ("--gc-expired-pending", &["--gc-expired-pending"][..]),
        ("--endpoints", &["--endpoints", "http://127.0.0.1:9"]),
        ("--ids", &["--ids", "1"]),
        ("--failure-domains", &["--failure-domains", "rack-a"]),
    ] {
        let (status, stderr) = reap_once_at(store.data_dir(), extra);

        assert!(
            stderr.contains(&format!(
                "--reap-abandoned-uploads cannot be combined with {flag}:"
            )),
            "{flag}: the refusal names both flags:\n{stderr}"
        );
        assert!(
            stderr.contains(&format!("{flag} would be silently ignored")),
            "{flag}: the refusal says why:\n{stderr}"
        );
        assert!(!status.success(), "{flag}: refused, so non-zero: {status}");
        assert_eq!(
            reported(&stderr),
            "nothing",
            "{flag}: no pass ran:\n{stderr}"
        );
        assert_eq!(store.snapshot(), before, "{flag}: the store is untouched");
    }

    // Without any of them the same store is reaped: the refusals above were the flags', not the
    // fixture's.
    store.reap_until(|store| session_gone(store, &id));
    assert_eq!(store.counted(), 0);
}

/// **A `--data-dir` with no store in it is refused, and none is created.** Pointed at a directory
/// that does not exist, or at one that exists and holds no `meta.redb`, the one-shot must not
/// create a store, find it empty and report that nothing is left to reap: that would be an
/// all-clear about a store that is not the cluster's. It names the path it looked at, exits
/// non-zero, reports no pass, and leaves the directory as it found it.
#[test]
fn a_data_dir_with_no_store_is_refused_and_none_is_created() {
    let parent = tempfile::tempdir().expect("temp dir");
    let absent = parent.path().join("no-such-data-dir");
    let empty = parent.path().join("empty-data-dir");
    std::fs::create_dir(&empty).expect("an empty dir");

    for dir in [&absent, &empty] {
        let (status, stderr) = reap_once_at(dir, &[]);

        let looked_at = dir.join("meta.redb");
        assert!(
            stderr.contains(&format!("no metadata store at {}", looked_at.display())),
            "{}: the refusal names the path:\n{stderr}",
            dir.display()
        );
        assert!(
            !status.success(),
            "{}: refused, so non-zero: {status}",
            dir.display()
        );
        assert_eq!(
            reported(&stderr),
            "nothing",
            "{}: no pass is reported:\n{stderr}",
            dir.display()
        );
        assert!(
            !looked_at.exists(),
            "{}: no store was created",
            dir.display()
        );
    }
    assert!(!absent.exists(), "the missing directory was not created");
    assert_eq!(
        std::fs::read_dir(&empty).expect("the empty dir").count(),
        0,
        "the empty directory is still empty"
    );
}

/// **A zero-length `meta.redb` is not a store, and is not made one.** An open that creates takes
/// an empty file for the place to put a new database: it would initialize one there, find nothing
/// in it, and report that nothing is left to reap — an all-clear about a store no cluster wrote,
/// behind a path that did exist. The one-shot names the path and what it found there, exits
/// non-zero, reports no pass, and leaves the file at zero bytes. Once a writer has made a store
/// of that same file, the one-shot runs on it and names it: the refusal was the empty file's.
#[test]
fn an_empty_store_file_is_refused_and_left_empty() {
    let store = Store::new();
    let file = store.data_dir().join("meta.redb");
    std::fs::File::create(&file).expect("a zero-length file");

    let (status, stderr) = store.reap_once();

    assert!(
        stderr.contains(&format!(
            "no metadata store at {}: the file is empty",
            file.display()
        )),
        "the refusal names the path and what is there:\n{stderr}"
    );
    assert!(!status.success(), "refused, so non-zero: {status}");
    assert_eq!(
        reported(&stderr),
        "nothing",
        "no pass is reported:\n{stderr}"
    );
    assert_eq!(
        std::fs::metadata(&file)
            .expect("the file is still there")
            .len(),
        0,
        "no database was initialized in it"
    );
    assert_eq!(
        std::fs::read_dir(store.data_dir())
            .expect("the data dir")
            .count(),
        1,
        "and nothing was put beside it"
    );

    // A writer's open is the one that makes a store of an empty file.
    assert!(store.snapshot().is_empty());
    assert!(std::fs::metadata(&file).expect("the store").len() > 0);
    let (status, stderr) = store.reap_once();
    assert!(status.success(), "{status}\n{stderr}");
    assert_eq!(reported(&stderr), "satisfied", "{stderr}");
    let outcome = stderr
        .lines()
        .find(|line| line.contains("multipart reaper pass "))
        .unwrap_or_else(|| panic!("the one-shot reports its pass:\n{stderr}"));
    assert!(
        outcome.ends_with(&store.named()),
        "the pass names the store it ran on: {outcome}"
    );
}
