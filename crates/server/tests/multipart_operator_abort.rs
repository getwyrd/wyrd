//! The operator **abort** of a multipart upload session (FU-6, part 1; proposal 0016 decision 3's
//! `Completing → Aborting` edge `0016:572`, the batch rows `0016:664-665`, implementation order
//! point 3 `0016:255-262`), driven through the built binary.
//!
//! Every leg seeds a real redb store with raw records, drops it (redb holds an exclusive lock),
//! runs `wyrd mpu abort --upload-id <ID> --data-dir <DIR>` as a subprocess with a bound, reopens
//! the store and asserts on what it durably holds and on the exit status and stderr. The teardown
//! that follows is #625's operator one-shot, `wyrd custodian --reap-abandoned-uploads
//! --metadata-backend redb --data-dir <DIR>`, run the same way: the command the abort prints. Both
//! commands are argv strings, so this file names no symbol the abort adds and compiles the same on
//! a tree without it. Both pass `--metadata-backend redb`, so a `WYRD_METADATA_BACKEND` in the
//! environment cannot point them at another store, and `<DIR>` holds a quote and a space, so the
//! printed command is only right if it is quoted.
//!
//! The sessions are stamped under a clock the reaper does not own (`clock_source: "logical"`), the
//! case 0016 names the operator the only exit for (`0016:255-262`): the reaper skips them.
//!
//! * **A** — an `Open@2` session with staged bytes, which the one-shot alone leaves where it is, is
//!   fenced by the abort to `Aborting@3` with `retire:bytes:s:<id>:2` and nothing else written;
//!   the one-shot then tears it down: records gone, `orphan:` marks on every byte, `mpuctl` one
//!   lower.
//! * **B** — a `Completing@3` session that already wrote `seg:<g>:3:*` is fenced to `Aborting@4`
//!   with `retire:bytes:s:<id>:3` and `retire:records:s:<id>:3` naming `seg:<g>:3:` only; a decoy
//!   `seg:<g>:5:*` range is named by no obligation (F18). The one-shot drains `seg:<g>:3:` empty
//!   and leaves the decoy byte for byte.
//! * **C** — a `Completed` tombstone and an absent session are refused with the handled exit 1 and
//!   a message naming the state (not the unknown-command exit 2), the store unchanged; an
//!   `Aborting` session is answered exit 0 with nothing written.
//! * **D** — one session per invocation, and a command line read strictly: a second
//!   `--upload-id`, a wildcard, a flag the verb does not take (a clock source among them), a
//!   repeated flag, and a value that is empty or spelled as a flag are each refused, exit 1,
//!   before anything is written — `--data-dir --bogus` too, though a store sits at `--bogus`.
//! * **E** — a `Completing` session whose `retire:records:` obligation key alone is already taken
//!   is refused, exit 1, naming that key, with nothing written.
//! * **F** — the two commands the abort prints, the one-shot and the stop condition for this
//!   upload, run as printed by `sh` from another directory and under another backend variable,
//!   tear X down though a second held session makes every pass `BLOCKED`, and leave that one as
//!   seeded. The abort also says what to do when a pass holds X itself: run the custodian over the
//!   whole fleet.
//! * **G** — from a working directory whose name is not UTF-8, a relative path the printed
//!   commands would carry is refused, exit 1, before the store is opened: the redb `--data-dir`,
//!   and with `--features fdb` the FoundationDB cluster file and external-client directory.
//!
//! Every command, a shell line included, runs under one bound that covers its stderr closing.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use bytes::Bytes;
use wyrd_core::metadata::{
    self, decode_orphan_mark, orphan_key, seg_key, seg_range_prefix, seggrp_key, ChunkRef,
    EcScheme, SegmentGroup, SegmentNonce, SegmentRecord, SEGGRP_MARKER,
};
use wyrd_core::multipart::{
    decode_admission_record, decode_retire_obligation, mpu_key, part_key, part_range, psum_key,
    psum_range, retire_key, retire_session_range, sidx_key, sidx_range, slot_key, slot_range,
    AdmissionRecord, OwnedEntry, PartNumber, PartRecord, PartScope, PartSummary, RetireMode,
    RetireToken, SessionRecord, SlotIndex, SlotRecord, StagedPlacement, UploadId,
    MAX_INFLIGHT_PARTS, MAX_PARTS_PER_SESSION, MAX_PART_CHUNKS, MAX_STAGED_CHUNKS, MPUCTL_KEY,
    W_REF,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, WriteBatch};

const WYRD: &str = env!("CARGO_BIN_EXE_wyrd");

/// The longest one command may take. Each acts on a handful of records; two minutes is generous
/// on a loaded CI host, and a hang past it fails the leg rather than the suite.
const COMMAND_BOUND: Duration = Duration::from_secs(120);

/// How many one-shots a teardown may take to converge. The drain's marks land in the first; the
/// terminal delete waits for the `retire:` obligations to drain, so it lands in the second or
/// third. Six leaves room without hiding a teardown that never converges.
const CONVERGE_RUNS: usize = 6;

/// The clock the sessions are stamped under: one the reaper does not read, so no window of its
/// judges them and only an operator can move an `Open` one.
const FOREIGN_CLOCK: &str = "logical";

/// A fixed stamp under [`FOREIGN_CLOCK`]. No wall clock is read in this file: the reaper never
/// judges a foreign-clocked stamp, and every lease below is far behind any wall clock.
const STAMP: u64 = 1_000;

// ───────────────────────────── the store, outside the process ─────────────────────────────

/// A redb store in a directory under a temp root, the commands run from that root.
struct Store {
    root: tempfile::TempDir,
    dir: PathBuf,
}

impl Store {
    /// A store whose directory name a shell would split and unquote: the printed teardown command
    /// names it right only if it quotes it.
    fn new() -> Self {
        Self::named("it's a store")
    }

    /// A store in `<root>/<name>`.
    fn named(name: &str) -> Self {
        let root = tempfile::tempdir().expect("temp dir");
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).expect("the store's directory");
        Self { root, dir }
    }

    fn data_dir(&self) -> &Path {
        &self.dir
    }

    fn data_dir_str(&self) -> &str {
        self.dir.to_str().expect("a UTF-8 temp path")
    }

    /// `wyrd <args>`, run from the temp root, with no `RUST_LOG` that could filter out the
    /// audit line.
    fn wyrd(&self, args: &[&str]) -> (ExitStatus, String) {
        let mut command = Command::new(WYRD);
        command
            .current_dir(self.root.path())
            .env_remove("RUST_LOG")
            .args(args);
        run_bounded(command)
    }

    /// Run `f` over the store, opened for the call and closed before it returns.
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

    /// `wyrd mpu abort --upload-id <id> --metadata-backend redb --data-dir <dir>`: its exit
    /// status and stderr.
    fn abort(&self, id: &str) -> (ExitStatus, String) {
        self.mpu_abort(&["--upload-id", id])
    }

    /// `wyrd mpu abort <args> --metadata-backend redb --data-dir <dir>`.
    fn mpu_abort(&self, args: &[&str]) -> (ExitStatus, String) {
        let mut argv = vec!["mpu", "abort"];
        argv.extend_from_slice(args);
        argv.extend([
            "--metadata-backend",
            "redb",
            "--data-dir",
            self.data_dir_str(),
        ]);
        self.wyrd(&argv)
    }

    /// The one-shot the abort prints for this store, as a shell would read it: `wyrd custodian
    /// --reap-abandoned-uploads --metadata-backend redb --data-dir '<dir>'`, the directory quoted.
    fn teardown_line(&self) -> String {
        format!(
            "wyrd custodian --reap-abandoned-uploads --metadata-backend redb --data-dir '{}'",
            self.data_dir_str().replace('\'', r"'\''")
        )
    }

    /// One #625 one-shot over this store: [`Self::teardown_line`], as argv.
    fn reap_once(&self) -> (ExitStatus, String) {
        self.wyrd(&[
            "custodian",
            "--reap-abandoned-uploads",
            "--metadata-backend",
            "redb",
            "--data-dir",
            self.data_dir_str(),
        ])
    }

    /// One-shots until `done` holds, at most [`CONVERGE_RUNS`]; every one must exit 0.
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

/// Run `command` to its end, within [`COMMAND_BOUND`]: its exit status and its stderr. The bound
/// covers the whole of it, the wait for its stderr to close included: a process that inherited
/// the pipe and outlived the child would otherwise hold that read open past any bound. The child
/// is killed and reaped on every way out ([`Killed`]), and leg F runs each shell line under
/// `exec`, so the child is the command itself and killing it leaves no such process.
fn run_bounded(mut command: Command) -> (ExitStatus, String) {
    // The test's own monotonic clock: it bounds this wait and judges nothing in the store.
    let deadline = Instant::now() + COMMAND_BOUND;
    let mut child = Killed(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn wyrd"),
    );
    // Read while the child runs: a child whose stderr fills the pipe must never block on it.
    let mut pipe = child.0.stderr.take().expect("stderr");
    let (sender, drained) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut stderr = String::new();
        // The receiver is gone only when the bound has already failed the test.
        let _ = sender.send(pipe.read_to_string(&mut stderr).map(|_| stderr));
    });
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("wait") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the command must return within {COMMAND_BOUND:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let stderr = drained
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .unwrap_or_else(|_| {
            panic!("the command's stderr must close within {COMMAND_BOUND:?} ({status})")
        })
        .expect("stderr is utf-8");
    (status, stderr)
}

/// The child process, killed and reaped on every exit path so a hang never outlives the test.
struct Killed(Child);

impl Drop for Killed {
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

/// `id`'s segment-group nonce: its hex reversed, so never the upload id itself.
fn nonce(id: &UploadId) -> String {
    id.as_str().chars().rev().collect()
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

/// `mpuctl` counting `count` sessions under the shipped profile, spelled from its public
/// constants: the profile the one-shot's custodian holds as its own.
fn ledger(count: u64) -> (Vec<u8>, Bytes) {
    let profile = format!(
        "{{\"w_ref\":{W_REF},\"max_part_chunks\":{MAX_PART_CHUNKS},\
         \"max_parts_per_session\":{MAX_PARTS_PER_SESSION},\
         \"max_inflight_parts\":{MAX_INFLIGHT_PARTS},\"max_staged_chunks\":{MAX_STAGED_CHUNKS}}}"
    );
    let budget: wyrd_core::multipart::Budget =
        metadata::decode(profile.as_bytes()).expect("a legal profile");
    let json = format!(
        "{{\"count\":{count},\"max_sessions\":{},\"profile\":{profile}}}",
        budget.max_sessions()
    );
    (MPUCTL_KEY.to_vec(), canonical::<AdmissionRecord>(&json))
}

/// `id`'s session record at `epoch` with `attempts`, in `state` (its JSON), under
/// [`FOREIGN_CLOCK`], in the codec's own spelling.
fn session(id: &UploadId, epoch: u64, attempts: u32, state: &str) -> Bytes {
    let json = format!(
        "{{\"parent\":42,\"object\":\"o\",\"created_at_millis\":{STAMP},\
         \"clock_source\":\"{FOREIGN_CLOCK}\",\"segment_nonce\":\"{}\",\"epoch\":{epoch},\
         \"attempts\":{attempts},\"state\":{state}}}",
        nonce(id)
    );
    canonical::<SessionRecord>(&json)
}

const OPEN: &str = "{\"kind\":\"Open\"}";
const ABORTING: &str = "{\"kind\":\"Aborting\"}";

/// A `Completing` state fenced at `epoch`, `written` segments durable.
fn completing(epoch: u64, written: u32) -> String {
    format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":{STAMP},\"segments_written\":{written},\
         \"publish_target\":{{\"parent\":42,\"name\":\"o\",\"epoch\":{epoch}}}}}"
    )
}

/// A `Completed` state: the tombstone of a published upload.
fn completed() -> String {
    format!(
        "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":7,\"version\":1,\"etag\":\
         \"{}-1\",\"completed_at_millis\":{STAMP},\"complete_fingerprint\":\"{}\"}}}}",
        "ab".repeat(32),
        "cd".repeat(32)
    )
}

/// A committed part `n` of `id` over `chunks`, committed under the session at `epoch`: its
/// `part:` and `psum:` records.
fn committed_part(id: &UploadId, n: u32, chunks: &[ChunkRef], epoch: u64) -> Vec<(Vec<u8>, Bytes)> {
    let digest = format!("{n:064x}");
    let len: u64 = chunks.iter().map(|c| c.len).sum();
    let chunks_json = String::from_utf8(metadata::encode(&chunks).to_vec()).unwrap();
    let part_json = format!(
        "{{\"chunks\":{chunks_json},\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{STAMP},\"session_epoch\":{epoch}}}"
    );
    let summary_json = format!(
        "{{\"chunks\":{},\"len\":{len},\"digest\":\"{digest}\",\"committed_at_millis\":{STAMP}}}",
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

/// Slot `index` of `id`, reserved for part `n`, its lease long lapsed.
fn slot(id: &UploadId, index: u32, n: u32) -> (Vec<u8>, Bytes) {
    let json = format!(
        "{{\"part_number\":{n},\"attempt_id\":\"{}\",\"reserved_at_millis\":{STAMP},\
         \"lease_expiry_millis\":{}}}",
        "ab".repeat(16),
        STAMP + 1
    );
    (
        slot_key(id, SlotIndex::new(index).expect("a slot index")),
        canonical::<SlotRecord>(&json),
    )
}

/// The owned staging entry of `chunk` for part `n` of `id`: a crashed in-flight part's residue.
fn owned(id: &UploadId, n: u32, chunk: &ChunkRef) -> (Vec<u8>, Bytes) {
    let staged = StagedPlacement::new(chunk.scheme, chunk.placement.clone()).expect("staged");
    let entry = OwnedEntry::new(id.clone(), STAMP + 1, staged).to_pending();
    (sidx_key(id, part(n), chunk.id), metadata::encode(&entry))
}

/// `count` segment records of `group`, from `first_chunk` on: what a completer wrote.
fn segments(group: &SegmentGroup, first_chunk: ChunkId, count: u32) -> Vec<(Vec<u8>, Bytes)> {
    (0..count)
        .map(|index| {
            let chunk = chunk(first_chunk + ChunkId::from(index), [1, 2, 3]);
            let record =
                SegmentRecord::new(vec![chunk], u64::from(index) * 4_096).expect("a segment");
            (
                seg_key(group, index).expect("a segment key"),
                metadata::encode(&record),
            )
        })
        .collect()
}

fn session_token(id: &UploadId, epoch: u64) -> RetireToken {
    RetireToken::Session {
        upload_id: id.clone(),
        epoch,
        part: None,
    }
}

/// Every `retire:` record in the store, decoded against its key.
fn obligations(store: &Store) -> Vec<(Vec<u8>, wyrd_core::multipart::RetirePayload)> {
    store
        .range(b"retire:")
        .into_iter()
        .map(|(key, value)| {
            let (_, _, payload) = decode_retire_obligation(&key, &value).unwrap_or_else(|fault| {
                panic!("{} decodes: {fault}", String::from_utf8_lossy(&key))
            });
            (key, payload)
        })
        .collect()
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

/// Assert an `orphan:` mark stands, stamped, on every position of `chunk`.
fn assert_marked(store: &Store, chunk: &ChunkRef) {
    for (dserver, frag) in positions(chunk) {
        let value = store
            .value(&orphan_key(dserver, frag))
            .unwrap_or_else(|| panic!("an orphan: mark on server {dserver}, {frag:?}"));
        let mark = decode_orphan_mark(&value).expect("the mark decodes");
        assert!(!mark.is_reclaiming(), "{frag:?} is stamped, not reclaiming");
    }
}

/// The keys at which `after` differs from `before`, either way.
fn changed(before: &BTreeMap<Vec<u8>, Bytes>, after: &BTreeMap<Vec<u8>, Bytes>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = before
        .keys()
        .chain(after.keys())
        .filter(|key| before.get(*key) != after.get(*key))
        .cloned()
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

fn names(keys: &[Vec<u8>]) -> Vec<String> {
    keys.iter()
        .map(|key| String::from_utf8_lossy(key).into_owned())
        .collect()
}

// ─────────────────────────────── the legs ───────────────────────────────

/// **A — the operator abort of an `Open` session, and the clock-free teardown after it.** An
/// `Open@2` session under a clock the reaper does not own holds a committed part and a crashed
/// in-flight part's residue (a slot and an owned entry). The one-shot alone leaves it as it is. The
/// abort exits 0 and writes exactly two keys: the session at `Aborting@3` and its
/// `retire:bytes:s:<id>:2` `{session, all}` obligation — it deletes nothing. The one-shot then
/// tears the upload down: every record gone, an `orphan:` mark on every byte, `mpuctl` one lower.
#[test]
fn abort_fences_an_open_session_and_the_one_shot_tears_it_down() {
    let store = Store::new();
    let id = upload(0xa1);
    let committed = chunk(0xa1_0001, [1, 2, 3]);
    let in_flight = chunk(0xa1_0002, [4, 5, 6]);
    let mut records = vec![
        ledger(1),
        (mpu_key(&id), session(&id, 2, 0, OPEN)),
        (
            seggrp_key(&nonce_of(&id)),
            Bytes::from_static(SEGGRP_MARKER),
        ),
        slot(&id, 0, 2),
        owned(&id, 2, &in_flight),
    ];
    records.extend(committed_part(&id, 1, std::slice::from_ref(&committed), 2));
    store.seed(records);

    // The defect: no in-system actor moves it. The reaper holds the foreign-clocked session.
    let before = store.snapshot();
    let (status, stderr) = store.reap_once();
    assert!(
        !status.success(),
        "the one-shot holds a foreign-clocked Open session: {status}\n{stderr}"
    );
    assert_eq!(store.snapshot(), before, "the one-shot alone moved nothing");

    let (status, stderr) = store.abort(id.as_str());

    assert_eq!(status.code(), Some(0), "the abort exits 0:\n{stderr}");
    assert!(
        stderr.contains(&format!("`{}`", store.teardown_line())),
        "the abort prints the one-shot for this store, quoted, which this leg then runs:\n{stderr}"
    );
    assert!(
        stderr.contains("\"action\":\"operator-abort-fenced\""),
        "the acknowledged fence is on the audit seam as this abort's:\n{stderr}"
    );
    let after = store.snapshot();
    let bytes_key = retire_key(RetireMode::Bytes, &session_token(&id, 2));
    assert_eq!(
        names(&changed(&before, &after)),
        names(&[mpu_key(&id), bytes_key.clone()]),
        "the abort writes the session and its one obligation, and deletes nothing"
    );
    assert_eq!(
        after.get(&mpu_key(&id)),
        Some(&session(&id, 3, 0, ABORTING)),
        "Open@2 is left Aborting@3"
    );
    let owed = obligations(&store);
    assert_eq!(owed.len(), 1, "one obligation in all: {owed:?}");
    let (key, payload) = &owed[0];
    assert_eq!(key, &bytes_key);
    assert!(
        payload.session() && payload.parts() == Some(&PartScope::All),
        "the Open door owes the staged residue and every part: {payload:?}"
    );
    assert_eq!(store.counted(), 1, "the abort does not touch the count");

    store.reap_until(|store| session_gone(store, &id));

    assert_torn_down(&store, &id);
    assert_eq!(store.counted(), 0, "the admission slot is released");
    assert_marked(&store, &committed);
    assert_marked(&store, &in_flight);
}

/// **B — the abort of a `Completing` session also retires that attempt's segments, and only
/// those.** A `Completing@3` session over two committed parts has written `seg:<g>:3:0..1`; a
/// decoy `seg:<g>:5:*` range sits under the same group nonce. The abort exits 0 and leaves the
/// session `Aborting@4` with `retire:bytes:s:<id>:3` `{session, all}` and
/// `retire:records:s:<id>:3` `{seg: <g, 3>}` — and no obligation anywhere names the decoy epoch
/// (F18). The one-shot drains `seg:<g>:3:` empty and leaves the decoy byte for byte.
#[test]
fn abort_of_a_completing_session_retires_that_attempts_segments_only() {
    let store = Store::new();
    let id = upload(0xb1);
    let group = SegmentGroup::new(nonce(&id), 3).expect("the attempt's group");
    let decoy = SegmentGroup::new(nonce(&id), 5).expect("a later epoch of the group");
    let parts = [chunk(0xb1_0001, [1, 2, 3]), chunk(0xb1_0002, [4, 5, 6])];
    let mut records = vec![
        ledger(1),
        (mpu_key(&id), session(&id, 3, 1, &completing(3, 2))),
        (
            seggrp_key(&nonce_of(&id)),
            Bytes::from_static(SEGGRP_MARKER),
        ),
    ];
    for (n, chunk) in (1..).zip(&parts) {
        records.extend(committed_part(&id, n, std::slice::from_ref(chunk), 2));
    }
    let attempt = segments(&group, 0xb1_0001, 2);
    let decoyed = segments(&decoy, 0xb1_0101, 2);
    records.extend(attempt.iter().cloned());
    records.extend(decoyed.iter().cloned());
    store.seed(records);
    assert_eq!(store.range(&seg_range_prefix(&group)).len(), 2);
    let before = store.snapshot();

    let (status, stderr) = store.abort(id.as_str());

    assert_eq!(status.code(), Some(0), "the abort exits 0:\n{stderr}");
    let after = store.snapshot();
    let bytes_key = retire_key(RetireMode::Bytes, &session_token(&id, 3));
    let records_key = retire_key(RetireMode::Records, &session_token(&id, 3));
    assert_eq!(
        names(&changed(&before, &after)),
        names(&[mpu_key(&id), bytes_key.clone(), records_key.clone()]),
        "the abort writes the session and its two obligations, and deletes nothing"
    );
    assert_eq!(
        after.get(&mpu_key(&id)),
        Some(&session(&id, 4, 1, ABORTING)),
        "Completing@3 is left Aborting@4"
    );
    let owed: BTreeMap<_, _> = obligations(&store).into_iter().collect();
    assert_eq!(owed.len(), 2, "two obligations in all: {owed:?}");
    let bytes = &owed[&bytes_key];
    assert!(
        bytes.session() && bytes.parts() == Some(&PartScope::All),
        "the Completing door owes the staged residue and the parts: {bytes:?}"
    );
    let segs = &owed[&records_key];
    assert_eq!(
        segs.segments(),
        Some(&group),
        "retire:records names seg:<g>:3: — the fence epoch's range: {segs:?}"
    );
    assert!(!segs.session() && segs.parts().is_none(), "{segs:?}");
    for (key, payload) in &owed {
        assert_ne!(
            payload.segments(),
            Some(&decoy),
            "{} names the decoy epoch (F18)",
            String::from_utf8_lossy(key)
        );
    }

    store.reap_until(|store| {
        session_gone(store, &id) && store.range(&seg_range_prefix(&group)).is_empty()
    });

    assert_torn_down(&store, &id);
    assert!(
        store.range(&seg_range_prefix(&group)).is_empty(),
        "the attempt's segments are drained"
    );
    assert_eq!(
        store.range(&seg_range_prefix(&decoy)),
        decoyed,
        "the decoy epoch's segments are left byte for byte"
    );
    assert_eq!(store.counted(), 0, "the admission slot is released");
    for chunk in &parts {
        assert_marked(&store, chunk);
    }
}

/// **C — the abort refuses where 0016 says it must, and says why.** A `Completed@4` tombstone and
/// an absent session are refused with the handled failure, exit 1 — not the unknown-command exit
/// 2 — and a message naming the state; an `Aborting@3` session is answered exit 0. None of the
/// three writes a byte: no second fence, no second obligation, the epoch unchanged.
#[test]
fn abort_refuses_terminal_and_absent_sessions_and_is_idempotent_on_aborting() {
    let store = Store::new();
    let (done, missing, aborting) = (upload(0xc1), upload(0xc2), upload(0xc3));
    store.seed(vec![
        ledger(2),
        (mpu_key(&done), session(&done, 4, 1, &completed())),
        (mpu_key(&aborting), session(&aborting, 3, 0, ABORTING)),
        (
            retire_key(RetireMode::Bytes, &session_token(&aborting, 2)),
            Bytes::from_static(b"{\"session\":true,\"parts\":\"all\"}"),
        ),
    ]);
    // The seeded obligation is the Open door's own spelling: it decodes against its key.
    assert_eq!(obligations(&store).len(), 1);
    let before = store.snapshot();

    let (status, stderr) = store.abort(done.as_str());
    assert_eq!(
        status.code(),
        Some(1),
        "a Completed session is refused, handled:\n{stderr}"
    );
    assert!(
        stderr.contains("Completed")
            && stderr.contains("abort does not apply to a terminal session"),
        "the refusal names the terminal state:\n{stderr}"
    );
    assert_eq!(store.snapshot(), before, "nothing written for Completed");

    let (status, stderr) = store.abort(missing.as_str());
    assert_eq!(
        status.code(),
        Some(1),
        "an absent session is refused, handled:\n{stderr}"
    );
    assert!(
        stderr.contains("absent")
            && stderr.contains(missing.as_str())
            && stderr.contains("nothing written"),
        "the refusal names the session as absent, nothing written:\n{stderr}"
    );
    assert_eq!(
        store.snapshot(),
        before,
        "nothing written for an absent session"
    );

    let (status, stderr) = store.abort(aborting.as_str());
    assert_eq!(
        status.code(),
        Some(0),
        "an Aborting session is idempotent success:\n{stderr}"
    );
    assert!(
        stderr.contains("already Aborting@3"),
        "the answer names the state found:\n{stderr}"
    );
    assert_eq!(
        store.snapshot(),
        before,
        "no second fence, no second obligation, the epoch unchanged"
    );
}

/// **D — one session per invocation, no bulk form, and a command line read strictly.** Each
/// invocation below is refused, exit 1, before anything is written: the `Open` sessions it could
/// have named are left as they were. A second `--upload-id` (also beside other repeated flags), a
/// wildcard and a missing `--upload-id`; a flag the verb does not take, a clock source among them;
/// and a value that is empty or spelled as a flag, which is not taken as the value, so a flag
/// cannot hide behind another (`--data-dir --gc-expired-pending --data-dir <dir>`).
#[test]
fn abort_names_exactly_one_session() {
    let store = Store::new();
    let (one, two) = (upload(0xd1), upload(0xd2));
    store.seed(vec![
        ledger(2),
        (mpu_key(&one), session(&one, 0, 0, OPEN)),
        (mpu_key(&two), session(&two, 0, 0, OPEN)),
    ]);
    let before = store.snapshot();
    let dir = store.data_dir_str();
    let backend = ["--metadata-backend", "redb"];

    for (args, says) in [
        (
            vec!["--upload-id", one.as_str(), "--upload-id", two.as_str()],
            "`--upload-id` is given more than once",
        ),
        (vec!["--upload-id", "*"], "not an upload id"),
        (vec![], "--upload-id ID is required"),
        (
            vec!["--upload-id", one.as_str(), "--clock-source", "logical"],
            "unknown flag `--clock-source`",
        ),
    ] {
        let (status, stderr) = store.mpu_abort(&args);
        assert_eq!(status.code(), Some(1), "{args:?} is refused:\n{stderr}");
        assert!(stderr.contains(says), "{args:?}: {stderr}");
        assert_eq!(store.snapshot(), before, "{args:?} wrote nothing");
    }

    // Whole command lines, each flag as written.
    for (args, says) in [
        (
            vec![
                "--data-dir",
                dir,
                "--data-dir",
                dir,
                "--upload-id",
                one.as_str(),
                "--upload-id",
                two.as_str(),
            ],
            "`--data-dir` is given more than once",
        ),
        (
            vec![
                "--log-level",
                "info",
                "--log-level",
                "info",
                "--upload-id",
                one.as_str(),
                "--upload-id",
                two.as_str(),
                "--data-dir",
                dir,
            ],
            "`--log-level` is given more than once",
        ),
        (
            vec!["--upload-id", one.as_str(), "--data-dir", ""],
            "flag `--data-dir` needs a value",
        ),
        (
            vec![
                "--upload-id",
                one.as_str(),
                "--data-dir",
                "--gc-expired-pending",
                "--data-dir",
                dir,
            ],
            "flag `--data-dir` needs a value",
        ),
    ] {
        let mut argv = vec!["mpu", "abort"];
        argv.extend_from_slice(&backend);
        argv.extend_from_slice(&args);
        let (status, stderr) = store.wyrd(&argv);
        assert_eq!(status.code(), Some(1), "{args:?} is refused:\n{stderr}");
        assert!(stderr.contains(says), "{args:?}: {stderr}");
        assert_eq!(store.snapshot(), before, "{args:?} wrote nothing");
    }

    // A value spelled as a flag is not a directory, even where one by that name holds a store
    // with the session in it: `--data-dir --bogus` is a missing value, refused.
    let bogus = Store::named("--bogus");
    bogus.seed(vec![ledger(1), (mpu_key(&one), session(&one, 0, 0, OPEN))]);
    let held = bogus.snapshot();
    let (status, stderr) = bogus.wyrd(&[
        "mpu",
        "abort",
        "--upload-id",
        one.as_str(),
        "--data-dir",
        "--bogus",
        "--metadata-backend",
        "redb",
    ]);
    assert_eq!(
        status.code(),
        Some(1),
        "--data-dir --bogus is refused:\n{stderr}"
    );
    assert!(
        stderr.contains("flag `--data-dir` needs a value"),
        "--data-dir --bogus: {stderr}"
    );
    assert_eq!(bogus.snapshot(), held, "the store at --bogus is left Open");
}

/// **E — a taken records obligation key is damage, named, and nothing is written.** A
/// `Completing@3` session whose `retire:bytes:s:<id>:3` key is free but whose
/// `retire:records:s:<id>:3` key already holds a record: the fence requires both absent, so it
/// loses, and with the session unchanged on both sides of the read that found the key, the abort
/// names that key — the second one its fence requires, not the first — and exits 1, the store
/// exactly as seeded.
#[test]
fn abort_names_a_taken_records_obligation_key_and_writes_nothing() {
    let store = Store::new();
    let id = upload(0xe1);
    let records_key = retire_key(RetireMode::Records, &session_token(&id, 3));
    store.seed(vec![
        ledger(1),
        (mpu_key(&id), session(&id, 3, 1, &completing(3, 0))),
        (records_key.clone(), Bytes::from_static(b"{}")),
    ]);
    let before = store.snapshot();

    let (status, stderr) = store.abort(id.as_str());

    assert_eq!(status.code(), Some(1), "the abort is refused:\n{stderr}");
    let named = String::from_utf8(records_key).expect("a UTF-8 key");
    assert!(
        stderr.contains(&format!("the obligation key {named} is already taken")),
        "the refusal names the records key:\n{stderr}"
    );
    assert_eq!(store.snapshot(), before, "nothing written");
}

/// **F — the commands the abort prints act on the store it acted on, from anywhere, and stop on
/// this upload.** Two `Open@2` sessions under the foreign clock, X and Y, each with a committed
/// part, in a store the abort is pointed at by a **relative** `--data-dir`, its backend chosen by
/// a `WYRD_METADATA_BACKEND` scoped to the abort's command alone. The abort of X exits 0 and
/// prints two commands: the one-shot, and the stop condition — this abort again, until it answers
/// X absent. Both are run as printed, by `sh`, from another working directory, with
/// `WYRD_METADATA_BACKEND` naming a backend no build has: each works only if it names the store's
/// absolute directory and its backend itself. Y, which no actor will move, makes every pass
/// `BLOCKED` (exit 1), and the passes still run X's teardown: once the printed abort answers X
/// absent, X is torn down and Y is byte for byte as seeded.
#[test]
fn the_printed_commands_tear_this_upload_down_from_anywhere_while_another_is_held() {
    let store = Store::new();
    let (x, y) = (upload(0xf1), upload(0xf2));
    let x_chunk = chunk(0xf1_0001, [1, 2, 3]);
    let y_chunk = chunk(0xf2_0001, [4, 5, 6]);
    let mut records = vec![ledger(2)];
    for (id, part_chunk) in [(&x, &x_chunk), (&y, &y_chunk)] {
        records.push((mpu_key(id), session(id, 2, 0, OPEN)));
        records.push((seggrp_key(&nonce_of(id)), Bytes::from_static(SEGGRP_MARKER)));
        records.extend(committed_part(id, 1, std::slice::from_ref(part_chunk), 2));
    }
    store.seed(records);
    let held = |store: &Store| {
        let mut owned = store.range(&part_range(&y));
        owned.extend(store.range(&psum_range(&y)));
        (store.value(&mpu_key(&y)), owned)
    };
    let y_seeded = held(&store);

    let relative = store
        .data_dir()
        .file_name()
        .and_then(|name| name.to_str())
        .expect("the store's directory name");
    let mut abort = Command::new(WYRD);
    abort
        .current_dir(store.root.path())
        .env("WYRD_METADATA_BACKEND", "redb")
        .env_remove("RUST_LOG")
        .args([
            "mpu",
            "abort",
            "--upload-id",
            x.as_str(),
            "--data-dir",
            relative,
        ]);
    let (status, stderr) = run_bounded(abort);
    assert_eq!(status.code(), Some(0), "the abort exits 0:\n{stderr}");
    let teardown = backquoted_after(&stderr, "run `");
    let probe = backquoted_after(&stderr, "until `");
    let label = between(&stderr, "[store: redb at ", "]");
    assert!(
        Path::new(&label).is_absolute(),
        "the store is named by its absolute path: {label}"
    );
    assert_eq!(
        std::fs::canonicalize(&label).expect("the named store exists"),
        std::fs::canonicalize(store.data_dir().join("meta.redb")).unwrap(),
        "the store named is the store the relative --data-dir resolved to"
    );
    for printed in [&teardown, &probe] {
        assert!(
            printed.contains("--metadata-backend redb --data-dir '/"),
            "a printed command names the backend and the absolute directory: {printed}"
        );
    }
    assert!(
        stderr.contains("BLOCKED"),
        "the abort says a BLOCKED pass over other uploads is no stall of this one:\n{stderr}"
    );
    assert!(
        stderr.contains(&format!(
            "one whose held lines name upload {x} (a partial-fleet hold, say) will not clear by \
             repeating that metadata-only one-shot: stop it, and run the custodian over the same \
             store without --reap-abandoned-uploads, with --endpoints, --ids and \
             --failure-domains for the whole fleet"
        )),
        "the abort says what to do when a pass holds this upload itself:\n{stderr}"
    );

    // Elsewhere, under a backend variable that would send a command without its own flag astray.
    // Under `exec`, so the shell becomes the command and the bound kills the command itself; a
    // redb line starts with the command's name, which `exec` takes.
    let elsewhere = tempfile::tempdir().expect("another working directory");
    let sh = |line: &str| {
        assert!(
            line.starts_with("wyrd "),
            "a redb line is the bare command: {line}"
        );
        let mut command = Command::new("sh");
        command
            .current_dir(elsewhere.path())
            .env("PATH", path_with_wyrd())
            .env("WYRD_METADATA_BACKEND", "no-such-backend")
            .env_remove("RUST_LOG")
            .args(["-c", &format!("exec {line}")]);
        run_bounded(command)
    };
    // The absent answer's own words: the Aborting answer quotes the stop condition, not these.
    let gone = format!("upload {x} is absent: no session record mpu:{x}");
    let mut passes = 0;
    loop {
        let (status, said) = sh(&probe);
        if said.contains(&gone) {
            assert_eq!(
                status.code(),
                Some(1),
                "absent is the handled refusal:\n{said}"
            );
            break;
        }
        assert_eq!(
            status.code(),
            Some(0),
            "the printed abort, run elsewhere, finds X still Aborting:\n{said}"
        );
        assert!(said.contains("already Aborting@3"), "{said}");
        assert!(
            passes < CONVERGE_RUNS,
            "X's teardown did not finish within {CONVERGE_RUNS} passes"
        );
        let (status, said) = sh(&teardown);
        passes += 1;
        assert_eq!(
            status.code(),
            Some(1),
            "Y, held, makes the printed one-shot's pass BLOCKED:\n{said}"
        );
        assert!(
            said.contains("BLOCKED") && said.contains(y.as_str()),
            "the pass names Y as what it held:\n{said}"
        );
        assert!(
            said.contains(&format!("[store: redb at {label}]")),
            "the pass ran over the store the abort acted on:\n{said}"
        );
    }

    assert!(passes >= 1, "the teardown took a pass of the one-shot");
    assert_torn_down(&store, &x);
    assert_marked(&store, &x_chunk);
    assert_eq!(
        store.counted(),
        1,
        "X's admission slot is released, Y's kept"
    );
    assert_eq!(held(&store), y_seeded, "Y is left byte for byte as seeded");
}

/// **G — a path the printed commands would carry, with no UTF-8 spelling, is refused before the
/// store is opened.** The abort runs from a working directory whose name is not UTF-8, so a
/// relative path resolves to an absolute one no printed command could spell. A relative redb
/// `--data-dir` there is refused, exit 1, naming the flag, and the store it names, which holds an
/// `Open` session, is left as seeded.
#[cfg(unix)]
#[test]
fn abort_refuses_a_data_dir_it_could_not_print_before_opening_the_store() {
    let root = tempfile::tempdir().expect("temp dir");
    let cwd = root.path().join(not_utf8());
    std::fs::create_dir(&cwd).expect("a directory whose name is not UTF-8");
    let store = Store {
        dir: cwd.join("store"),
        root,
    };
    std::fs::create_dir(store.data_dir()).expect("the store's directory");
    let id = upload(0x91);
    store.seed(vec![ledger(1), (mpu_key(&id), session(&id, 2, 0, OPEN))]);
    let before = store.snapshot();

    let mut abort = Command::new(WYRD);
    abort.current_dir(&cwd).env_remove("RUST_LOG").args([
        "mpu",
        "abort",
        "--upload-id",
        id.as_str(),
        "--metadata-backend",
        "redb",
        "--data-dir",
        "store",
    ]);
    let (status, stderr) = run_bounded(abort);

    assert_eq!(status.code(), Some(1), "the abort is refused:\n{stderr}");
    assert!(
        stderr.contains("--data-dir resolves to") && stderr.contains("which is not UTF-8"),
        "the refusal names the flag and why:\n{stderr}"
    );
    assert_eq!(store.snapshot(), before, "the store is left as seeded");
}

/// **G, FoundationDB** (`--features fdb`): the cluster file and the external-client directory
/// the printed commands carry are made absolute the same way, and each is refused the same way,
/// exit 1, naming its variable, before the store is opened: no connection is tried, so no cluster
/// is needed, and the cluster file named does not exist.
#[cfg(all(unix, feature = "fdb"))]
#[test]
fn abort_refuses_an_fdb_path_it_could_not_print_before_connecting() {
    let root = tempfile::tempdir().expect("temp dir");
    let cwd = root.path().join(not_utf8());
    std::fs::create_dir(&cwd).expect("a directory whose name is not UTF-8");
    let id = upload(0x92);
    for (cluster_file, external_client_dir, named) in [
        ("fdb.cluster", None, "WYRD_FDB_CLUSTER_FILE"),
        (
            "/no-such-dir/fdb.cluster",
            Some("clients"),
            "WYRD_FDB_EXTERNAL_CLIENT_DIR",
        ),
    ] {
        let mut abort = Command::new(WYRD);
        abort
            .current_dir(&cwd)
            .env_remove("RUST_LOG")
            .env_remove("WYRD_FDB_EXTERNAL_CLIENT_DIR")
            .env("WYRD_FDB_CLUSTER_FILE", cluster_file)
            // A build that reached for the cluster anyway fails its probe in a second, not ten.
            .env("WYRD_FDB_TRANSACTION_TIMEOUT_MS", "1000")
            .args([
                "mpu",
                "abort",
                "--upload-id",
                id.as_str(),
                "--metadata-backend",
                "fdb",
            ]);
        if let Some(dir) = external_client_dir {
            abort.env("WYRD_FDB_EXTERNAL_CLIENT_DIR", dir);
        }
        let (status, stderr) = run_bounded(abort);

        assert_eq!(status.code(), Some(1), "{named}: refused:\n{stderr}");
        assert!(
            stderr.contains(&format!("{named} resolves to"))
                && stderr.contains("which is not UTF-8"),
            "the refusal names {named} and why:\n{stderr}"
        );
        assert!(
            !stderr.contains("fdb backend"),
            "{named}: refused before any connection is tried:\n{stderr}"
        );
    }
}

/// A directory name that is not UTF-8.
#[cfg(unix)]
fn not_utf8() -> &'static std::ffi::OsStr {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::OsStr::from_bytes(b"not-utf8-\xff")
}

/// The first `` `…` `` span after `marker` in `text`.
fn backquoted_after(text: &str, marker: &str) -> String {
    between(text, marker, "`")
}

/// The text between the first `open` in `text` and the next `close` after it.
fn between(text: &str, open: &str, close: &str) -> String {
    let start = text
        .find(open)
        .unwrap_or_else(|| panic!("`{open}` in:\n{text}"))
        + open.len();
    let len = text[start..]
        .find(close)
        .unwrap_or_else(|| panic!("`{close}` after `{open}` in:\n{text}"));
    text[start..start + len].to_owned()
}

/// `PATH` with the built `wyrd`'s directory first, so a printed `wyrd …` line runs this build.
fn path_with_wyrd() -> std::ffi::OsString {
    let bin = Path::new(WYRD).parent().expect("the binary's directory");
    let rest = std::env::var_os("PATH").unwrap_or_default();
    std::env::join_paths(std::iter::once(bin.to_path_buf()).chain(std::env::split_paths(&rest)))
        .expect("a PATH")
}

/// `id`'s nonce as the validated type `seggrp_key` takes.
fn nonce_of(id: &UploadId) -> SegmentNonce {
    SegmentNonce::new(nonce(id)).expect("32 lowercase hex characters")
}
