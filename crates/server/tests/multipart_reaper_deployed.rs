//! The multipart **reaper runs on every deployed startup shape** of the ordinary `wyrd custodian`
//! role (#625, slice 625.2; proposal 0016 decision 6, `0016:1794-2277`, and the requirement that
//! the reaper run wherever the verbs are exposed, `0016:234-268`).
//!
//! Each leg seeds a real redb store **exactly as `multipart_reaper.rs` leg A does** — an idle
//! `Open` session created 14 days ago, its progress stamped 10 days ago, every lease lapsed 9 days
//! ago, one committed `part:`/`psum:` pair, the `sidx:` residue of a crashed in-flight part and
//! `mpuctl.count = 1` — drops it (redb holds an exclusive lock), runs the built binary as a
//! subprocess with **no one-shot flag anywhere in argv**, bounds it, reopens the store and asserts
//! leg A's transition: every `mpu:`, `part:`, `psum:`, `sidx:` and `slot:` record of the session
//! gone, the count back to zero, an `orphan:` mark on every position of both byte sets, and both of
//! the session's `retire:` ranges empty.
//!
//! How many reaper passes a run made is read off its stderr: every pass emits its
//! `monotonic_counter.reaper_passes` tally (`wyrd_custodian` reaper, `emit_pass`) through the
//! role's log sink, one line a pass. The level is pinned with `--log-level info`, so a caller's
//! `RUST_LOG` cannot hide it.
//!
//! * **G2a — no `--endpoints`.** The role runs the reaper alone, every `--interval-secs`, instead
//!   of exiting. ONE run of the role, never restarted: it is left up until it has made
//!   [`PASSES_IN_ONE_RUN`] passes, then sent Ctrl-C, and must exit cleanly. Leg A's teardown takes
//!   more than one pass (the terminal delete waits for the `retire:` obligation to drain), so a
//!   store that converged within one process lifetime is a store that process reaped on more than
//!   one interval. A role that reaped once at startup and then idled never reaches the pass count,
//!   and leaves the teardown unfinished. The passes must also be an interval apart: each tally is
//!   timed as it arrives, and the first and the last of them must lie at least
//!   `PASSES_IN_ONE_RUN - 1` intervals apart, less [`ARRIVAL_SLACK`], so a loop that ignores
//!   `--interval-secs` and runs pass after pass fails here. While the role is up, a second `wyrd`
//!   on the same data directory is refused the store: the role holds redb's exclusive lock.
//! * **G2a, the first pass.** A Ctrl-C sent during the first pass, over [`FIRST_PASS_UPLOADS`]
//!   idle uploads, lets that pass finish: the role exits 0 after exactly one pass that fenced
//!   every upload. A role whose Ctrl-C is registered only after its first pass dies of the signal
//!   there. The Ctrl-C is sent with the role stopped (SIGSTOP) and a mark written into its stderr,
//!   so that it reached the role inside the pass is read off the role's own output — no pass
//!   tally before the mark — and never off when this test happened to read a line.
//! * **G2c — `--endpoints` given, none reachable.** The role runs exactly one reaper pass and then
//!   fails as it always has: the panic's exit status and the existing "NO configured D server was
//!   reachable at startup" diagnostic. Every run is checked for all three — exactly one pass
//!   emitted, that status, the diagnostic — and the first run must have moved the session
//!   off the record it was seeded with. Leg A's transition is then asserted after the role has been
//!   started again until it converged, as a supervisor restarts it (`Restart=on-failure`): one pass
//!   a start cannot finish a teardown that takes more than one pass, so the transition the brief
//!   asks of G2c is reached across starts, never inside one.
//! * **G2c under `--reconcile-after-restore`.** The same empty fleet, but the restore one-shot: it
//!   fails as before, and the store is byte-identical — no reaper pass runs ahead of a restore
//!   pass, which fences what the restore resurrected under its own generation.
//! * **No store, no fleet.** Pointed at a data directory that holds no store, neither shape
//!   without a fleet creates one: the no-endpoints role is refused, non-zero, naming the path;
//!   the empty-fleet role fails as it always has. No `meta.redb` appears in either.
//! * **Refusals.** The no-endpoints role is refused, before it touches the store, with a flag
//!   only a fleet uses (`--gc-expired-pending`, `--ids`, `--failure-domains`) and with
//!   `--interval-secs 0`. Only that shape refuses 0: given `--endpoints` the role takes it as it
//!   always has — on an empty fleet, one reaper pass and then the same failure — and the reaper
//!   one-shot, which never waits, takes it too.
//!
//! Every command selects the redb backend explicitly (`--metadata-backend redb`) and drops
//! `WYRD_METADATA_BACKEND` from the child's environment, so a caller configured for a networked
//! store can never point these destructive passes at it instead of the temporary fixture.
//!
//! The seeding helpers are copied from `multipart_reaper.rs`, not shared: a shared
//! `tests/support/mod.rs` would be classified as a test target of its own. The command is stringly
//! typed, so this file names no symbol the slice adds and compiles the same on a tree without it.
//!
//! Plain `#[test]`s with short-lived runtimes of their own: the role `block_on`s its own.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, PipeWriter, Write};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
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
use wyrd_custodian::reaper::SESSIONS_PER_PASS;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, WriteBatch};

const WYRD: &str = env!("CARGO_BIN_EXE_wyrd");

/// The one-shot's flag. Asserted absent from every argv this file runs: the legs prove the
/// ordinary role reaps, not the operator one-shot.
const ONE_SHOT_FLAG: &str = "--reap-abandoned-uploads";

/// The role's interval in these legs, in seconds: the shortest the role accepts.
const INTERVAL_SECS: &str = "1";

/// [`INTERVAL_SECS`] as a duration, for G2a's spacing check.
const INTERVAL: Duration = Duration::from_secs(1);

/// How much earlier than the role wrote it a pass's tally may have been timed, in G2a's spacing
/// check: the reader thread times each line as it reads it, so only the first tally's read can
/// shorten the measured span, never the last's. Far below the interval a back-to-back loop would
/// skip.
const ARRIVAL_SLACK: Duration = Duration::from_millis(500);

/// How many reaper passes G2a waits for in its one run before terminating the role. Leg A's
/// teardown lands within three passes (the fence and the drain's marks, then the drain, then the
/// terminal delete); a pass starts only once the one before it has finished, so a fifth pass
/// emitted means four have run to their end — one to spare.
const PASSES_IN_ONE_RUN: usize = 5;

/// The longest any one run may take: G2a's wait for its passes, and G2c's wait for the role to
/// exit by itself. A hang past it fails the leg rather than the suite.
const RUN_BOUND: Duration = Duration::from_secs(120);

/// The longest the role may take to exit once it has been sent Ctrl-C: the rest of the pass it is
/// in, then the shutdown. A role that ignores the Ctrl-C fails the leg here.
const SHUTDOWN_BOUND: Duration = Duration::from_secs(30);

/// How many starts G2c allows the store to converge in. One pass a start; leg A's teardown takes
/// two or three. Six leaves room without hiding a role that never converges.
const CONVERGE_RUNS: usize = 6;

/// The diagnostic the empty-fleet startup has always panicked with (`cli.rs`, `cmd_custodian`).
const EMPTY_FLEET_DIAGNOSTIC: &str = "NO configured D server was reachable at startup";

/// A D-server endpoint nothing listens on: the dial is refused at once.
const UNREACHABLE: &str = "http://127.0.0.1:1";

/// The tally every reaper pass emits once, at its end.
const PASS_TALLY: &str = "monotonic_counter.reaper_passes";

/// The line the reaper logs as each fence it makes commits (`wyrd_custodian` reaper,
/// `emit_fenced`): a line written inside a pass.
const FENCED: &str = "the multipart reaper fenced an abandoned upload to Aborting";

/// How many idle uploads the first-pass leg seeds. Fewer than one pass lists
/// ([`SESSIONS_PER_PASS`]), so the first pass fences every one of them — one commit each — and
/// how far it got is countable; enough that the role, stopped once its first fence line is read,
/// still has most of that pass to run.
const FIRST_PASS_UPLOADS: u8 = 48;

/// The line [`interrupt_frozen`] writes into the role's stderr while the role is stopped: what
/// the role wrote before it stopped lies before this line, and the rest after it.
const FROZEN: &str = "-- test harness: the role is stopped here, and its Ctrl-C is sent now --";

/// The longest the role may take to stop once it has been sent SIGSTOP: a thread inside a system
/// call stops when that call returns.
const FREEZE_BOUND: Duration = Duration::from_secs(30);

/// How many runs the first-pass leg may make to stop the role inside its first pass. A role that
/// had already finished that pass when it stopped proves nothing about a Ctrl-C during it, so
/// that run is set aside — on its stderr, never on its outcome — and the leg runs again, over a
/// fresh store.
const FIRST_PASS_RUNS: usize = 5;

/// The status a Rust process exits with when its main thread panics: what the empty-fleet
/// startup has always exited with.
const PANIC_STATUS: i32 = 101;

/// What redb answers a second open of a store another process holds open: the store's exclusive
/// lock, as a `wyrd` command reports it.
const STORE_LOCKED: &str = "Database already open";

const DAY: u64 = 24 * 60 * 60 * 1_000;

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

/// A redb store under a temp dir the role is pointed at.
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
}

// ─────────────────────────────── the role, as a subprocess ───────────────────────────────

/// The ordinary role's command over `data_dir`, with `extra` after it — never the one-shot, and
/// always the redb store under `data_dir` — at the one-second interval.
fn role(data_dir: &Path, extra: &[&str]) -> Command {
    role_every(data_dir, INTERVAL_SECS, extra)
}

/// [`role`] at `--interval-secs interval`.
fn role_every(data_dir: &Path, interval: &str, extra: &[&str]) -> Command {
    let command = custodian(data_dir, interval, extra);
    let argv: Vec<String> = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert!(
        !argv.iter().any(|arg| arg == ONE_SHOT_FLAG),
        "the ordinary role, not the one-shot: {argv:?}"
    );
    command
}

/// `wyrd custodian` over the redb store under `data_dir`, at `--interval-secs interval`, with
/// `extra` after it. The backend is pinned by flag and `WYRD_METADATA_BACKEND` dropped, so no
/// caller's environment can point the run at another store.
fn custodian(data_dir: &Path, interval: &str, extra: &[&str]) -> Command {
    let mut command = Command::new(WYRD);
    command
        .arg("custodian")
        .arg("--data-dir")
        .arg(data_dir)
        .args(["--metadata-backend", "redb"])
        .args(["--log-level", "info"])
        .args(["--interval-secs", interval])
        .args(extra)
        .env_remove("WYRD_METADATA_BACKEND");
    command
}

/// The role on an empty fleet: one configured D server, nothing listening at its endpoint.
fn empty_fleet_role(data_dir: &Path, extra: &[&str]) -> Command {
    empty_fleet_role_every(data_dir, INTERVAL_SECS, extra)
}

/// [`empty_fleet_role`] at `--interval-secs interval`.
fn empty_fleet_role_every(data_dir: &Path, interval: &str, extra: &[&str]) -> Command {
    let mut args = vec![
        "--endpoints",
        UNREACHABLE,
        "--ids",
        "1",
        "--failure-domains",
        "domain-1",
        "--connect-timeout-secs",
        "1",
    ];
    args.extend_from_slice(extra);
    role_every(data_dir, interval, &args)
}

/// When [`run_role`] sends the role Ctrl-C, if ever.
#[derive(Clone, Copy)]
enum Interrupt {
    /// Never: the role runs until it exits by itself, or until [`RUN_BOUND`].
    Never,
    /// Once its stderr shows this many reaper passes.
    AfterPasses(usize),
    /// At the first stderr line that contains this text, with the role held stopped there
    /// while the Ctrl-C is sent ([`interrupt_frozen`]), so where the role stood when the Ctrl-C
    /// reached it is read off its stderr exactly.
    FrozenAtLine(&'static str),
}

/// What one run of the role did: its exit status if it exited — by itself, or after the Ctrl-C
/// [`run_role`] sent it — whether it was sent that Ctrl-C, its stderr, how many reaper passes
/// that stderr shows, and when each of those tallies was read.
struct Run {
    exited: Option<ExitStatus>,
    interrupted: bool,
    stderr: String,
    passes: usize,
    pass_times: Vec<Instant>,
}

/// Send the role `signal` through the system's `kill` (this file forbids `unsafe`, so not
/// `libc::kill`). A role that cannot be signalled fails the leg.
fn signal(child: &Child, signal: &str) {
    let status = Command::new("kill")
        .args(["-s", signal, &child.id().to_string()])
        .status()
        .unwrap_or_else(|e| panic!("run `kill` to send the role SIG{signal}: {e}"));
    assert!(
        status.success(),
        "SIG{signal} delivered to the role (kill exited {status})"
    );
}

/// Send the role Ctrl-C — SIGINT, what `wyrd custodian` stops on.
fn interrupt(child: &Child) {
    signal(child, "INT");
}

/// Whether every thread of the role is stopped: state `T` in each `/proc/<pid>/task/<tid>/stat`
/// (`<tid> (<name>) <state> …`). A thread still inside a system call — a commit's `fsync` — reads
/// as something else until that call returns, and stops before it runs any more of the role.
fn every_thread_stopped(child: &Child) -> bool {
    let Ok(threads) = std::fs::read_dir(format!("/proc/{}/task", child.id())) else {
        return false;
    };
    let mut seen = 0;
    for thread in threads {
        let Ok(stat) =
            thread.and_then(|thread| std::fs::read_to_string(thread.path().join("stat")))
        else {
            return false;
        };
        let state = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.trim_start().chars().next());
        if state != Some('T') {
            return false;
        }
        seen += 1;
    }
    seen > 0
}

/// Send the role Ctrl-C at a point of its run that is known exactly, not inferred from when
/// this process got round to reading a line. The role is stopped (SIGSTOP), and once every one
/// of its threads is seen stopped, [`FROZEN`] is written into the role's own stderr pipe through
/// `mark`: a stopped role writes nothing, so every line it wrote before it stopped lies before
/// that mark and every line it writes once continued lies after it. Then SIGINT is sent — it
/// stays pending on a stopped process — and the role is continued (SIGCONT), which delivers the
/// SIGINT at the very point the role had reached.
fn interrupt_frozen(child: &Child, mark: &mut PipeWriter) {
    signal(child, "STOP");
    let asked = Instant::now();
    while !every_thread_stopped(child) {
        assert!(
            asked.elapsed() < FREEZE_BOUND,
            "the role did not stop within {FREEZE_BOUND:?} of SIGSTOP"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    writeln!(mark, "{FROZEN}").expect("mark the role's stderr where it stopped");
    interrupt(child);
    signal(child, "CONT");
}

/// Whether `line` is a reaper pass's tally: the counter's name, followed by anything but more of
/// a name.
fn is_pass_tally(line: &str) -> bool {
    line.match_indices(PASS_TALLY).any(|(at, _)| {
        line[at + PASS_TALLY.len()..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'))
    })
}

fn passes_in(stderr: &str) -> usize {
    stderr.lines().filter(|line| is_pass_tally(line)).count()
}

/// Run `command` until it exits, or until [`RUN_BOUND`]. Unless `when` is [`Interrupt::Never`],
/// the role is sent Ctrl-C as `when` says, and is then given [`SHUTDOWN_BOUND`] to exit. Whatever
/// is still up at the end is killed.
fn run_role(command: Command, when: Interrupt) -> Run {
    run_role_and(command, when, &|| {})
}

/// [`run_role`], calling `while_up` once, just before the Ctrl-C of [`Interrupt::AfterPasses`]
/// is sent: the role is up, between two passes or inside one.
fn run_role_and(mut command: Command, when: Interrupt, while_up: &dyn Fn()) -> Run {
    // The role's stderr is a pipe this process made, so that it can hold a write end too: the
    // mark of [`interrupt_frozen`] goes down the same pipe the role writes, in order with it.
    let (pipe, role_end) = std::io::pipe().expect("a pipe for the role's stderr");
    let mut mark = match when {
        Interrupt::FrozenAtLine(_) => Some(role_end.try_clone().expect("a second write end")),
        Interrupt::Never | Interrupt::AfterPasses(_) => None,
    };
    let mut child = Reaped(
        command
            .stdout(Stdio::null())
            .stderr(role_end)
            .spawn()
            .expect("spawn wyrd"),
    );
    // `command` holds the write end it gave the role. Dropped, so the pipe closes — and the
    // reader below ends — once the role has exited and `mark` is gone.
    drop(command);
    // Read while the child runs, a line at a time: a child whose stderr fills the pipe must never
    // block on it, and the passes are counted as they are emitted.
    // Each line is timed by the reader as it reads it, not when this loop gets to it.
    let (lines, received) = mpsc::channel::<(Instant, String)>();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            let Ok(line) = line else { break };
            if lines.send((Instant::now(), line)).is_err() {
                break;
            }
        }
    });
    let started = Instant::now();
    let mut interrupted_at: Option<Instant> = None;
    let mut stderr = String::new();
    let mut passes = 0;
    let mut pass_times = Vec::new();
    let exited = loop {
        match received.recv_timeout(Duration::from_millis(50)) {
            Ok((read_at, line)) => {
                if is_pass_tally(&line) {
                    passes += 1;
                    pass_times.push(read_at);
                }
                let now = match when {
                    Interrupt::Never => false,
                    Interrupt::AfterPasses(wanted) => passes >= wanted,
                    Interrupt::FrozenAtLine(text) => line.contains(text),
                };
                stderr.push_str(&line);
                stderr.push('\n');
                if interrupted_at.is_none() && now {
                    match mark.take() {
                        // Its write end is dropped with the mark written, so the pipe still
                        // closes when the role exits.
                        Some(mut mark) => interrupt_frozen(&child.0, &mut mark),
                        None => {
                            while_up();
                            interrupt(&child.0);
                        }
                    }
                    interrupted_at = Some(Instant::now());
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            // The pipe closed: the child is on its way out.
            Err(RecvTimeoutError::Disconnected) => std::thread::sleep(Duration::from_millis(50)),
        }
        if let Some(status) = child.0.try_wait().expect("wait") {
            break Some(status);
        }
        if started.elapsed() >= RUN_BOUND
            || interrupted_at.is_some_and(|at| at.elapsed() >= SHUTDOWN_BOUND)
        {
            break None;
        }
    };
    // Killed here when it is still up; the pipe then closes and the reader ends. A mark never
    // written — the role never reached its line — must not hold the pipe open.
    drop(child);
    drop(mark);
    reader.join().expect("the stderr reader");
    for (read_at, line) in received.try_iter() {
        if is_pass_tally(&line) {
            pass_times.push(read_at);
        }
        stderr.push_str(&line);
        stderr.push('\n');
    }
    Run {
        exited,
        interrupted: interrupted_at.is_some(),
        passes: passes_in(&stderr),
        pass_times,
        stderr,
    }
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

/// Leg A's upload and its two byte sets.
struct LegA {
    id: UploadId,
    committed: ChunkRef,
    in_flight: ChunkRef,
    /// The session record as seeded: `Open@0`.
    seeded_session: Bytes,
}

/// Seed `store` exactly as `multipart_reaper.rs` leg A does, at the wall clock `now`: an idle
/// `Open` session only the idle arm can reap — created 14 days ago (past `W_open`, inside
/// `W_session`), its newest progress stamp 10 days old, every lease lapsed 9 days ago — with one
/// committed part and the residue of a crashed in-flight part (a slot and an owned entry, no part
/// record); `mpuctl.count = 1`.
fn seed_leg_a(store: &Store, now: u64) -> LegA {
    let id = upload(0xa1);
    let committed = chunk(0xa1_0001, [1, 2, 3]);
    let in_flight = chunk(0xa1_0002, [4, 5, 6]);
    let session = open_session(&id, now - 14 * DAY, "wall");
    let seeded_session = session.1.clone();
    let mut records = vec![
        ledger(1, W_REF),
        session,
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
    LegA {
        id,
        committed,
        in_flight,
        seeded_session,
    }
}

/// Seed `store` with `n` idle uploads, each in leg A's shape — created 14 days before the wall
/// clock `now`, its progress stamped 10 days before, every lease lapsed 9 days before, one
/// committed part and a crashed part's residue — each over byte sets of its own, with
/// `mpuctl.count = n`. Returns each upload with its session record as seeded (`Open@0`).
fn seed_idle_uploads(store: &Store, now: u64, n: u8) -> Vec<(UploadId, Bytes)> {
    let mut records = vec![ledger(u64::from(n), W_REF)];
    let mut seeded = Vec::new();
    for i in 0..n {
        // Away from leg A's 0xa1, and one byte each.
        let tag = 0x10 + i;
        let id = upload(tag);
        let base = ChunkId::from(tag) << 16;
        let committed = chunk(base | 1, [1, 2, 3]);
        let in_flight = chunk(base | 2, [4, 5, 6]);
        let session = open_session(&id, now - 14 * DAY, "wall");
        seeded.push((id.clone(), session.1.clone()));
        records.push(session);
        records.push(slot(&id, 0, 2, now - 10 * DAY, now - 9 * DAY));
        records.push(owned(&id, 2, &in_flight, now - 9 * DAY));
        records.extend(committed_part(
            &id,
            1,
            std::slice::from_ref(&committed),
            now - 10 * DAY,
        ));
    }
    store.seed(records);
    assert_eq!(store.counted(), u64::from(n));
    seeded
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

/// Assert leg A's transition: the session torn down, its admission slot released (the count back
/// to its pre-session zero), and every position of both byte sets licensed.
fn assert_leg_a_transition(store: &Store, leg: &LegA) {
    assert_torn_down(store, &leg.id);
    assert_eq!(store.counted(), 0, "the admission slot is released");
    assert_licensed(store, &positions(&leg.committed));
    assert_licensed(store, &positions(&leg.in_flight));
}

// ─────────────────────────────── the legs ───────────────────────────────

/// **G2a — the no-endpoints role reaps every interval, in one lifetime.** Started with no
/// `--endpoints`, a one-second interval and no one-shot flag, the role is left up — one run, never
/// restarted — until it has emitted [`PASSES_IN_ONE_RUN`] reaper passes, then sent Ctrl-C. It must
/// still have been up then (it does not exit with nothing to reconstruct), it must have made those
/// passes in that one process, the Ctrl-C must end it cleanly, and leg A's idle upload must be torn
/// down with every byte evidenced.
///
/// While it is up it holds the redb store's exclusive lock, as the usage text says: just before
/// the Ctrl-C, a second `wyrd` pointed at the same data directory — the reaper one-shot, which
/// opens the store as any command does — must be refused the store and run no pass.
#[test]
fn no_endpoints_role_reaps_every_interval_in_one_lifetime() {
    let store = Store::new();
    let leg = seed_leg_a(&store, wall_now());

    let locked_out = || {
        let second = run_role(
            custodian(store.data_dir(), INTERVAL_SECS, &[ONE_SHOT_FLAG]),
            Interrupt::Never,
        );
        assert!(
            second.exited.is_some_and(|status| !status.success()),
            "a second wyrd on the data dir of a running no-endpoints role is refused, non-zero \
             ({:?}):\n{}",
            second.exited,
            second.stderr
        );
        assert!(
            second.stderr.contains(STORE_LOCKED),
            "because the role holds the store's lock (`{STORE_LOCKED}`):\n{}",
            second.stderr
        );
        assert_eq!(
            second.passes, 0,
            "so it ran no reaper pass:\n{}",
            second.stderr
        );
    };
    let run = run_role_and(
        role(store.data_dir(), &[]),
        Interrupt::AfterPasses(PASSES_IN_ONE_RUN),
        &locked_out,
    );

    // The store first: it is what the role exists to change.
    assert!(
        session_gone(&store, &leg.id),
        "one run of the no-endpoints role did not finish leg A's teardown (it made {} reaper \
         pass(es); exited by itself: {:?}):\n{}",
        run.passes,
        run.exited,
        run.stderr
    );
    assert_leg_a_transition(&store, &leg);
    assert!(
        run.interrupted,
        "the no-endpoints role runs until stopped; it ended ({:?}) after {} pass(es), before it was \
         sent Ctrl-C:\n{}",
        run.exited,
        run.passes,
        run.stderr
    );
    assert!(
        run.passes >= PASSES_IN_ONE_RUN,
        "one process made the passes, one an interval: {} of {PASSES_IN_ONE_RUN} within \
         {RUN_BOUND:?}:\n{}",
        run.passes,
        run.stderr
    );
    // An interval apart, not back to back: the first pass runs at once and each later one an
    // interval after the one before it ended, so the first and the last tally lie at least
    // `PASSES_IN_ONE_RUN - 1` intervals apart.
    let span = run.pass_times[PASSES_IN_ONE_RUN - 1] - run.pass_times[0];
    let floor = INTERVAL * (PASSES_IN_ONE_RUN as u32 - 1) - ARRIVAL_SLACK;
    assert!(
        span >= floor,
        "{PASSES_IN_ONE_RUN} passes in {span:?}, where --interval-secs {INTERVAL_SECS} spaces them \
         at least {floor:?} apart: the role does not wait its interval between passes:\n{}",
        run.stderr
    );
    assert!(
        run.exited.is_some_and(|status| status.success()),
        "Ctrl-C ends the role cleanly, exit 0, within {SHUTDOWN_BOUND:?} ({:?}):\n{}",
        run.exited,
        run.stderr
    );
    assert!(
        run.stderr.contains("wyrd custodian: shutting down"),
        "and says so:\n{}",
        run.stderr
    );
}

/// **G2a, the first pass — a Ctrl-C sent during it lets it finish.** The role's Ctrl-C is
/// registered before its first pass, so one sent while that pass runs is held until the pass
/// ends, and the role then exits 0, as it does for one sent during any later pass. The store holds
/// [`FIRST_PASS_UPLOADS`] idle uploads in leg A's shape, and the role is stopped where it stands
/// once its first fence line is read, sent Ctrl-C there, and continued ([`interrupt_frozen`]).
///
/// That the Ctrl-C reached the role inside its first pass is read off the role's own stderr, not
/// off when this test read a line: the stopped role wrote nothing, so the lines before the
/// [`FROZEN`] mark are exactly what it had written when the Ctrl-C was sent — at least one fence,
/// and no pass tally, which a pass writes at its end. A run in which the role had already written
/// that tally says nothing about a Ctrl-C during the pass; it is set aside and the leg runs again,
/// up to [`FIRST_PASS_RUNS`] times, and fails if none of them stopped the role inside the pass.
///
/// Stopped inside it, the role must exit 0, saying it is shutting down, after exactly that one
/// pass, and that pass must have fenced every upload — it was not cut short. A role whose Ctrl-C
/// is registered only after its first pass dies of the signal there instead.
#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "reads each thread's state from /proc, which only Linux has"
)]
fn no_endpoints_role_finishes_its_first_pass_when_stopped_during_it() {
    assert!(usize::from(FIRST_PASS_UPLOADS) < SESSIONS_PER_PASS);
    let mut set_aside = Vec::new();
    for attempt in 1..=FIRST_PASS_RUNS {
        let store = Store::new();
        let seeded = seed_idle_uploads(&store, wall_now(), FIRST_PASS_UPLOADS);

        let run = run_role(role(store.data_dir(), &[]), Interrupt::FrozenAtLine(FENCED));

        let Some((before, _)) = run.stderr.split_once(FROZEN) else {
            panic!(
                "the role fenced nothing, so it was never stopped inside a pass ({:?}):\n{}",
                run.exited, run.stderr
            );
        };
        let fenced_before = before.lines().filter(|line| line.contains(FENCED)).count();
        assert!(
            fenced_before >= 1,
            "the role is stopped after the fence line that asked for it:\n{}",
            run.stderr
        );
        if passes_in(before) > 0 {
            set_aside.push(format!(
                "run {attempt}: the role had fenced {fenced_before} upload(s) and written its \
                 first pass's tally before it stopped"
            ));
            continue;
        }

        // The Ctrl-C reached the role inside its first pass, with that pass's tally — and, of
        // its fences, all but `fenced_before` — still to come.
        assert!(
            run.exited.is_some_and(|status| status.success()),
            "a Ctrl-C during the first pass ends the role cleanly, exit 0, once that pass is done \
             — not by the signal ({:?}; stopped after {fenced_before} fence(s)):\n{}",
            run.exited,
            run.stderr
        );
        assert!(
            run.stderr.contains("wyrd custodian: shutting down"),
            "and says so:\n{}",
            run.stderr
        );
        assert_eq!(
            run.passes, 1,
            "it stops after the pass it was in, and starts no other:\n{}",
            run.stderr
        );
        let fences = run
            .stderr
            .lines()
            .filter(|line| line.contains(FENCED))
            .count();
        assert_eq!(
            fences,
            usize::from(FIRST_PASS_UPLOADS),
            "that pass fenced every upload — it ran to its end:\n{}",
            run.stderr
        );
        for (id, session) in &seeded {
            assert_ne!(
                store.value(&mpu_key(id)).as_ref(),
                Some(session),
                "upload {id} is still as seeded: the first pass did not reach it"
            );
        }
        return;
    }
    panic!(
        "in none of {FIRST_PASS_RUNS} runs was the role stopped inside its first pass, so none \
         shows what a Ctrl-C during that pass does:\n{}",
        set_aside.join("\n")
    );
}

/// **G2c — the empty-fleet role runs exactly one reaper pass, then fails as before.** Started with
/// `--endpoints` naming one address nothing listens on, matching `--ids` / `--failure-domains`, a
/// one-second connect timeout and no one-shot flag. Every start must exit non-zero — with the
/// panic's own status, [`PANIC_STATUS`], as it always has — and with the existing diagnostic,
/// having emitted exactly one reaper pass; the first must have moved the session off its seeded
/// `Open` record. Restarted as a supervisor would, until the store converges, leg A's transition
/// must hold.
#[test]
fn empty_fleet_role_runs_one_reaper_pass_then_fails_as_before() {
    let store = Store::new();
    let leg = seed_leg_a(&store, wall_now());

    let mut faults = Vec::new();
    let mut starts = 0;
    while starts < CONVERGE_RUNS && !session_gone(&store, &leg.id) {
        let run = run_role(empty_fleet_role(store.data_dir(), &[]), Interrupt::Never);
        let mut fault = Vec::new();
        match run.exited {
            // Non-zero, and the very status it has always failed with: the panic's.
            Some(status) if status.code() == Some(PANIC_STATUS) => {}
            Some(status) => fault.push(format!(
                "exited {status}, where the empty fleet has always exited {PANIC_STATUS}"
            )),
            None => fault.push(format!("did not exit within {RUN_BOUND:?}")),
        }
        if !run.stderr.contains(EMPTY_FLEET_DIAGNOSTIC) {
            fault.push(format!("no \"{EMPTY_FLEET_DIAGNOSTIC}\" diagnostic"));
        }
        if run.passes != 1 {
            fault.push(format!(
                "{} reaper passes, where the empty fleet runs exactly one",
                run.passes
            ));
        }
        if starts == 0 && store.value(&mpu_key(&leg.id)).as_ref() == Some(&leg.seeded_session) {
            fault.push("its one pass left the session as seeded".to_owned());
        }
        if !fault.is_empty() {
            faults.push(format!(
                "start {starts}: {}\n{}",
                fault.join("; "),
                run.stderr
            ));
        }
        starts += 1;
    }

    // The store first: it is what the pass exists to change, and the status is non-zero on a tree
    // without the pass too.
    assert!(
        session_gone(&store, &leg.id),
        "leg A's teardown did not converge within {CONVERGE_RUNS} starts of the empty-fleet \
         role\n{}",
        faults.join("\n")
    );
    assert_leg_a_transition(&store, &leg);
    assert!(starts > 0, "the role was started");
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

/// **G2c under `--reconcile-after-restore` — the store is left untouched.** The restore one-shot,
/// on the same empty fleet, fails as it always has, and runs no reaper pass first: its store is a
/// just-restored image that only the restore pass may act on, under its own restore-fence
/// generation. Every byte of the store is as seeded.
#[test]
fn empty_fleet_restore_one_shot_fails_with_the_store_untouched() {
    let store = Store::new();
    seed_leg_a(&store, wall_now());
    let before = store.snapshot();

    let run = run_role(
        empty_fleet_role(store.data_dir(), &["--reconcile-after-restore"]),
        Interrupt::Never,
    );

    assert_eq!(
        store.snapshot(),
        before,
        "the restore one-shot on an empty fleet changed the store before failing:\n{}",
        run.stderr
    );
    assert!(
        run.exited.is_some_and(|status| !status.success()),
        "it fails, non-zero, as before ({:?}):\n{}",
        run.exited,
        run.stderr
    );
    assert!(
        run.stderr.contains(EMPTY_FLEET_DIAGNOSTIC),
        "with the same diagnostic:\n{}",
        run.stderr
    );
    assert_eq!(run.passes, 0, "and no reaper pass:\n{}", run.stderr);
}

/// **The no-endpoints role creates no store.** Pointed at a data directory that holds none, it is
/// refused, non-zero, with the path it looked at — and no `meta.redb` is made there. A store made
/// on the spot at a mistyped path would be reaped, empty, for good, while the cluster's went
/// unreaped.
#[test]
fn no_endpoints_role_refuses_a_data_dir_with_no_store() {
    let store = Store::new();
    let file = store.data_dir().join("meta.redb");

    let run = run_role(role(store.data_dir(), &[]), Interrupt::Never);

    assert!(
        !file.exists(),
        "the no-endpoints role created {}:\n{}",
        file.display(),
        run.stderr
    );
    assert!(
        run.exited.is_some_and(|status| !status.success()),
        "it is refused, non-zero ({:?}):\n{}",
        run.exited,
        run.stderr
    );
    assert!(
        run.stderr.contains("no metadata store at")
            && run.stderr.contains(&file.display().to_string()),
        "naming the path it found no store at:\n{}",
        run.stderr
    );
    assert_eq!(run.passes, 0, "and no reaper pass:\n{}", run.stderr);
}

/// **The empty-fleet role creates no store either.** On a data directory that holds none, its one
/// pass has nothing to open, and the role fails exactly as before — non-zero, the same diagnostic
/// — with no `meta.redb` made.
#[test]
fn empty_fleet_role_creates_no_store() {
    let store = Store::new();
    let file = store.data_dir().join("meta.redb");

    let run = run_role(empty_fleet_role(store.data_dir(), &[]), Interrupt::Never);

    assert!(
        !file.exists(),
        "the empty-fleet role created {}:\n{}",
        file.display(),
        run.stderr
    );
    assert!(
        run.exited.is_some_and(|status| !status.success()),
        "it fails, non-zero, as before ({:?}):\n{}",
        run.exited,
        run.stderr
    );
    assert!(
        run.stderr.contains(EMPTY_FLEET_DIAGNOSTIC),
        "with the same diagnostic:\n{}",
        run.stderr
    );
}

/// **A flag only a fleet uses is refused with no `--endpoints`.** The role with no fleet runs the
/// reaper alone for good, so `--gc-expired-pending` (GC runs only over a fleet), `--ids` and
/// `--failure-domains` would each be read and dropped. Each is refused, non-zero, naming the flag,
/// before the store is touched.
#[test]
fn no_endpoints_role_refuses_a_flag_only_a_fleet_uses() {
    let store = Store::new();
    seed_leg_a(&store, wall_now());
    let before = store.snapshot();

    for extra in [
        &["--gc-expired-pending"][..],
        &["--ids", "1"][..],
        &["--failure-domains", "domain-1"][..],
    ] {
        let run = run_role(role(store.data_dir(), extra), Interrupt::Never);
        let refusal = format!("{} needs --endpoints", extra[0]);
        assert!(
            run.exited.is_some_and(|status| !status.success()),
            "{extra:?} with no --endpoints is refused, non-zero ({:?}):\n{}",
            run.exited,
            run.stderr
        );
        assert!(
            run.stderr.contains(&refusal),
            "the refusal says `{refusal}`:\n{}",
            run.stderr
        );
        assert_eq!(run.passes, 0, "{extra:?}: no reaper pass:\n{}", run.stderr);
        assert_eq!(
            store.snapshot(),
            before,
            "{extra:?}: the store is as seeded:\n{}",
            run.stderr
        );
    }
}

/// **`--interval-secs 0` is refused with no `--endpoints`, and only there.** With no fleet the
/// role would run pass after pass with no pause, so it is refused before the store is touched.
/// Given `--endpoints` the role takes 0 as it always has: on a fleet none of whose servers is
/// reachable it is not refused, runs its one reaper pass, and fails exactly as at any other
/// interval — the same diagnostic, the same status, [`PANIC_STATUS`]. The reaper one-shot never
/// waits, so it takes 0 and runs its pass.
#[test]
fn a_zero_interval_is_refused_with_no_endpoints_and_only_there() {
    const REFUSAL: &str = "--interval-secs must be at least 1";
    let store = Store::new();
    seed_leg_a(&store, wall_now());
    let before = store.snapshot();

    let run = run_role(role_every(store.data_dir(), "0", &[]), Interrupt::Never);
    assert!(
        run.exited.is_some_and(|status| !status.success()),
        "with no --endpoints: refused, non-zero ({:?}):\n{}",
        run.exited,
        run.stderr
    );
    assert!(
        run.stderr.contains(REFUSAL),
        "the refusal says `{REFUSAL}`:\n{}",
        run.stderr
    );
    assert_eq!(run.passes, 0, "no reaper pass:\n{}", run.stderr);
    assert_eq!(store.snapshot(), before, "the store is as seeded");

    // The empty fleet, over a store of its own: its one pass changes it.
    let fleet_store = Store::new();
    let leg = seed_leg_a(&fleet_store, wall_now());
    let run = run_role(
        empty_fleet_role_every(fleet_store.data_dir(), "0", &[]),
        Interrupt::Never,
    );
    assert!(
        !run.stderr.contains(REFUSAL),
        "with --endpoints the role takes --interval-secs 0 as it always has:\n{}",
        run.stderr
    );
    assert_eq!(
        run.exited.and_then(|status| status.code()),
        Some(PANIC_STATUS),
        "and on an empty fleet fails with the status it always has ({:?}):\n{}",
        run.exited,
        run.stderr
    );
    assert!(
        run.stderr.contains(EMPTY_FLEET_DIAGNOSTIC),
        "and the same diagnostic:\n{}",
        run.stderr
    );
    assert_eq!(run.passes, 1, "after its one reaper pass:\n{}", run.stderr);
    assert_ne!(
        fleet_store.value(&mpu_key(&leg.id)).as_ref(),
        Some(&leg.seeded_session),
        "which moved the session off its seeded record"
    );

    let run = run_role(
        custodian(store.data_dir(), "0", &[ONE_SHOT_FLAG]),
        Interrupt::Never,
    );
    assert!(
        !run.stderr.contains(REFUSAL),
        "the one-shot takes --interval-secs 0:\n{}",
        run.stderr
    );
    assert!(
        run.exited.is_some_and(|status| status.success()),
        "and runs its pass ({:?}):\n{}",
        run.exited,
        run.stderr
    );
    assert_eq!(run.passes, 1, "one pass:\n{}", run.stderr);
}

/// **The usage text says what the role does with no fleet.** `wyrd` with no command prints the
/// usage and exits 2. Its custodian paragraph says that with no `--endpoints` the role runs the
/// multipart reaper alone until Ctrl-C over a store that must exist, that on redb it holds that
/// store's exclusive lock for as long as it runs, which flags it refuses there — an interval of 0
/// among them — and that when none of the `--endpoints` is reachable it runs one reaper pass —
/// none under `--reconcile-after-restore` — and then fails.
#[test]
fn usage_says_what_the_role_does_with_no_fleet() {
    let run = run_role(Command::new(WYRD), Interrupt::Never);

    assert_eq!(
        run.exited.and_then(|status| status.code()),
        Some(2),
        "{}",
        run.stderr
    );
    for said in [
        "it then runs the multipart reaper alone, every --interval-secs, until Ctrl-C,",
        "over a metadata store that must already exist (on redb it creates none). On",
        "redb it holds the --data-dir store's exclusive lock for as long as it runs, so",
        "no other wyrd process can open that store until it stops. It is refused with",
        "--ids, --failure-domains or --gc-expired-pending, which only a fleet uses, and with",
        "--interval-secs 0. If none of the --endpoints is reachable at startup, the role",
        "runs one reaper pass (none under --reconcile-after-restore) and then fails,",
        "non-zero, as before.",
    ] {
        assert!(
            run.stderr.contains(said),
            "the usage says `{said}`:\n{}",
            run.stderr
        );
    }
}
