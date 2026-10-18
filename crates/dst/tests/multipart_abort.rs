//! **Multipart Abort under commit ambiguity** — concurrent `CreateMultipartUpload`s
//! (`wyrd_core::multipart::create::create_session`, #894) and concurrent `AbortMultipartUpload`s
//! (`wyrd_core::multipart::abort::abort_session`, #895), two of them racing for some sessions, on
//! the simulated-FoundationDB store with the commit-ambiguity nemesis armed, inside the
//! deterministic simulator. A new concurrent path lands with seeded Tier-0 DST coverage
//! (`AGENTS.md`, review rubric, *Test fidelity*).
//!
//! The racing legs run [`CREATORS`] tasks. Each creates its session and then aborts it: an even
//! task with two concurrent Aborts, an odd one with one. The nemesis strikes more commits than a
//! seed makes, so nearly every commit comes back ambiguous and some Aborts spend all their
//! rounds. One leg per shape of the ambiguity class (`crates/metadata-fdb/src/lib.rs:159-166`):
//!
//! * **1021** — the transaction is out of flight; a re-read settles it.
//! * **1031** — the batch may land after the Abort's re-read, or later still, at
//!   [`SimFdbMetadataStore::quiesce`].
//! * **mixed** — each strike's code is a seed-derived coin, so one Abort can meet a 1031 and
//!   then a 1021.
//! * **witness** — the sessions are created first with the nemesis disarmed; then each gets one
//!   Abort with 1031 armed, on a store whose in-flight batches land only at `quiesce`
//!   ([`SimFdbMetadataStore::defer_in_flight_landings`]). An Abort whose every fence stays in
//!   flight then spends all its rounds and ends in doubt, and its fence lands after it returned.
//!   The sweep must see that at least once: it is the case the in-doubt error exists for.
//!
//! Every Abort runs on its own store wrapper, which records the fault history of **that
//! invocation** — never keyed by session and operation, which would merge two concurrent Aborts
//! of one session. After every seed, on the store after `quiesce`:
//!
//! 1. the number of `mpu:` records equals `mpuctl.count()`: an Abort never moves the counter;
//! 2. every `Aborting` session has its `retire:bytes:` `{session, all}` obligation, decoding
//!    against its own key, and no obligation is without its fenced session;
//! 3. a session none of whose Aborts answered `Fenced` or `AlreadyAborting`, and none of whose
//!    Aborts ended with an error saying a fence may still land, is still `Open`; one an Abort
//!    answered `Fenced` or `AlreadyAborting` for is `Aborting`;
//! 4. every Abort ended in one of the endings this scenario can reach: `Fenced`,
//!    `AlreadyAborting`, `NoSuchUpload` (for a create still in doubt), in doubt with a fence that
//!    may still land, or out of rounds with nothing in doubt. Never `OperationAborted`, never a
//!    damage fault, and never an out-of-flight outcome left unsettled: every read here succeeds,
//!    so one always settles it.
//!
//! **Demonstrated red — invariant (3) is load-bearing.** A store wrapper reports each struck 1031
//! to the Abort as `Conflict`, which the trait forbids, since `Conflict` asserts nothing was
//! written. In the witness leg the Abort then finds the session unchanged and the key absent,
//! spends its rounds, and says no fence of it can land, while a fence it sent is still in flight
//! and lands at `quiesce`: a fenced session no Abort reported. Invariant (3) catches it.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`, which sweeps 50 seeds over the
//! `#[madsim::test]` entry point); without it this file compiles to nothing.

#![forbid(unsafe_code)]
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self, InodeId, SegmentNonce};
use wyrd_core::multipart::abort::{abort_session, AbortError, Stop, ABORT_ROUNDS};
use wyrd_core::multipart::create::{create_session, CreateError, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    decode_admission_record, decode_retire_obligation, decode_session_record, parse_mpu_key,
    retire_key, AbortOutcome, Budget, CreateOutcome, PartScope, Refusal, RetireMode, RetireToken,
    SessionRecord, SessionState, UploadId, MPUCTL_KEY, MPU_PREFIX, RETIRE_BYTES_PREFIX,
    RETIRE_RECORDS_PREFIX,
};
use wyrd_traits::{
    CommitOutcome, CommitUnknownResult, MetadataStore, Result as StoreResult, ScanPage, WriteBatch,
};

#[path = "support/mod.rs"]
mod support;
use support::{
    FdbFidelity, FdbObservations, SimFdbMetadataStore, SIM_COMMIT_UNKNOWN_RESULT,
    SIM_TRANSACTION_TIMED_OUT,
};

const BUCKET: &str = "photos";
const ROOT: InodeId = 0;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
const CREATED_AT: u64 = 1_000;
const CLOCK: &str = "wall";
/// Sessions per seed: one creator task each.
const CREATORS: usize = 6;
/// The ledger's room: every creator fits, so a refusal is contention, never a full ledger.
const ROOM: u64 = 8;
/// Commits the nemesis strikes per seed: more than a seed makes (six creates, nine Aborts of at
/// most [`ABORT_ROUNDS`] commits each), so nearly every commit comes back ambiguous.
const STRIKES: u64 = 128;
/// Seeds each plain-`#[test]` sweep drives.
const SWEEP_SEEDS: u64 = 64;

/// Which shape of the ambiguity class a leg arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Unknown1021,
    TimedOut1031,
    Mixed,
    /// Sessions created with the nemesis disarmed, then one Abort each with 1031 armed, and every
    /// in-flight batch landing only at `quiesce`.
    Witness,
}

/// What an Abort's store wrapper tells it about a struck commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Report {
    /// The truth: the simulator's own `CommitUnknownResult`.
    Faithful,
    /// **Violating**: a struck 1031 reported as `Conflict`.
    TimedOut1031AsConflict,
}

/// A profile admitting exactly `n` sessions: every per-session cap at 1, so `U_ref` is 2.
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

/// One Abort invocation's store: the shared simulator, with the codes of the struck commits
/// **this invocation** met recorded in order, and reported to it as [`Report`] says.
struct Aborter {
    inner: Arc<SimFdbMetadataStore>,
    report: Report,
    codes: Mutex<Vec<i32>>,
    commits: Mutex<u32>,
}

#[async_trait]
impl MetadataStore for Aborter {
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
        *self.commits.lock().unwrap() += 1;
        let result = self.inner.commit(batch).await;
        let Err(err) = &result else {
            return result;
        };
        let unknown = err
            .downcast_ref::<CommitUnknownResult>()
            .unwrap_or_else(|| {
                panic!("a simulated-FDB commit returned a non-ambiguous Err: {err}")
            });
        let code = unknown.code.expect("the simulator names its code");
        self.codes.lock().unwrap().push(code);
        match (self.report, code) {
            (Report::TimedOut1031AsConflict, SIM_TRANSACTION_TIMED_OUT) => {
                Ok(CommitOutcome::Conflict)
            }
            _ => result,
        }
    }
}

/// The creator's runtime: identities and jitter from a seed-derived stream (never the wall
/// clock, never thread scheduling), and the backoff wait on madsim's virtual clock.
struct SimRuntime {
    rng: rand_chacha::ChaCha8Rng,
}

impl SimRuntime {
    fn next(&mut self) -> u64 {
        use rand::Rng;
        self.rng.next_u64()
    }

    fn token(&mut self) -> String {
        format!("{:016x}{:016x}", self.next(), self.next())
    }
}

impl CreateRuntime for SimRuntime {
    fn mint(&mut self) -> (UploadId, SegmentNonce) {
        (
            UploadId::new(self.token()).unwrap(),
            SegmentNonce::new(self.token()).unwrap(),
        )
    }

    fn jitter(&mut self) -> u64 {
        self.next()
    }

    fn wait(&mut self, millis: u64) -> impl Future<Output = ()> + Send {
        madsim::time::sleep(Duration::from_millis(millis))
    }
}

/// How one Abort invocation ended.
struct Ended {
    upload_id: UploadId,
    outcome: Result<AbortOutcome, AbortError>,
    /// The codes of the struck commits this invocation met, in order.
    codes: Vec<i32>,
    /// Fence commits this invocation made.
    commits: u32,
}

impl Ended {
    /// It answered that the session is fenced: `Fenced` or `AlreadyAborting`.
    fn reported_fence(&self) -> bool {
        matches!(
            self.outcome,
            Ok(AbortOutcome::Fenced | AbortOutcome::AlreadyAborting)
        )
    }

    /// It ended with an error saying a fence of it may still land.
    fn may_still_land(&self) -> bool {
        matches!(&self.outcome, Err(AbortError::InDoubt(doubt)) if doubt.may_still_commit)
    }

    /// It made all its fence commits and the session was still `Open`.
    fn spent_every_round(&self) -> bool {
        let cause = match &self.outcome {
            Err(AbortError::InDoubt(doubt)) => &doubt.cause,
            Err(AbortError::Stopped { cause, .. }) => cause,
            Ok(_) => return false,
        };
        matches!(cause, Stop::RoundsSpent { rounds } if *rounds == ABORT_ROUNDS)
            && self.commits == ABORT_ROUNDS
    }

    /// It met a 1031 and, later, a 1021.
    fn met_1031_then_1021(&self) -> bool {
        self.codes
            .iter()
            .position(|code| *code == SIM_TRANSACTION_TIMED_OUT)
            .is_some_and(|first| self.codes[first..].contains(&SIM_COMMIT_UNKNOWN_RESULT))
    }
}

/// Run `n` concurrent Aborts of `upload_id`, each on its own [`Aborter`].
async fn aborts(
    meta: &Arc<SimFdbMetadataStore>,
    upload_id: UploadId,
    n: usize,
    report: Report,
) -> Vec<Ended> {
    let tasks: Vec<_> = (0..n)
        .map(|_| {
            let store = Aborter {
                inner: Arc::clone(meta),
                report,
                codes: Mutex::new(Vec::new()),
                commits: Mutex::new(0),
            };
            let upload_id = upload_id.clone();
            madsim::task::spawn(async move {
                let outcome = abort_session(&store, &upload_id).await;
                Ended {
                    upload_id,
                    outcome,
                    codes: store.codes.into_inner().unwrap(),
                    commits: store.commits.into_inner().unwrap(),
                }
            })
        })
        .collect();
    let mut ended = Vec::new();
    for task in tasks {
        ended.push(task.await.unwrap());
    }
    ended
}

/// Creator `i`'s create: the upload id it told its caller about, by `Created` or by an in-doubt
/// error, if any.
async fn create(meta: &SimFdbMetadataStore, i: usize, seed: u64) -> Option<UploadId> {
    let object = format!("{BUCKET}/object-{i}");
    let request = CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object: &object,
        content_type: None,
        created_at_millis: CREATED_AT,
        clock_source: CLOCK,
    };
    let mut runtime = SimRuntime {
        rng: rand::SeedableRng::seed_from_u64(seed ^ (0x9e37_79b9 * (i as u64 + 1))),
    };
    match create_session(meta, &profile_of(ROOM), &request, &mut runtime).await {
        Ok(CreateOutcome::Created { upload_id }) => Some(upload_id),
        Err(CreateError::InDoubt(doubt)) => Some(doubt.upload_id),
        Ok(CreateOutcome::Refused(_)) | Err(CreateError::IdentityCollisions { .. }) => None,
        Err(other) => panic!("{object}: an unexpected create end in the simulator: {other}"),
    }
}

/// What one seed of a leg left behind.
struct Swept {
    observations: FdbObservations,
    ended: Vec<Ended>,
    /// Sessions two Aborts raced for.
    raced: usize,
    /// Aborts that ended saying a fence may still land, whose session is `Aborting` after
    /// `quiesce`: a fence that landed after its Abort returned, and was reported.
    held_and_landed: usize,
}

/// The scenario body: the leg's creates and Aborts with its nemesis armed, then `quiesce`, then
/// the invariants.
async fn abort_under_ambiguity(meta: Arc<SimFdbMetadataStore>, leg: Leg, report: Report) -> Swept {
    // The fixture's commit is made with the nemesis disarmed.
    let marker = WriteBatch::new().put(metadata::bucket_key(BUCKET), MARKER);
    meta.commit(marker).await.unwrap();
    let seed = madsim::runtime::Handle::current().seed();
    let mut ended = Vec::new();
    let mut raced = 0;
    if leg == Leg::Witness {
        let mut ids = Vec::new();
        for i in 0..CREATORS {
            ids.extend(create(&meta, i, seed).await);
        }
        meta.defer_in_flight_landings();
        meta.arm_commit_ambiguity(SIM_TRANSACTION_TIMED_OUT, STRIKES);
        let tasks: Vec<_> = ids
            .into_iter()
            .map(|id| {
                let meta = Arc::clone(&meta);
                madsim::task::spawn(async move { aborts(&meta, id, 1, report).await })
            })
            .collect();
        for task in tasks {
            ended.extend(task.await.unwrap());
        }
    } else {
        match leg {
            Leg::Unknown1021 => meta.arm_commit_ambiguity(SIM_COMMIT_UNKNOWN_RESULT, STRIKES),
            Leg::TimedOut1031 => meta.arm_commit_ambiguity(SIM_TRANSACTION_TIMED_OUT, STRIKES),
            Leg::Mixed => meta.arm_mixed_commit_ambiguity(STRIKES),
            Leg::Witness => unreachable!(),
        }
        let tasks: Vec<_> = (0..CREATORS)
            .map(|i| {
                let meta = Arc::clone(&meta);
                madsim::task::spawn(async move {
                    let Some(id) = create(&meta, i, seed).await else {
                        return Vec::new();
                    };
                    let n = if i % 2 == 0 { 2 } else { 1 };
                    aborts(&meta, id, n, report).await
                })
            })
            .collect();
        for task in tasks {
            let mine = task.await.unwrap();
            if mine.len() == 2 {
                raced += 1;
            }
            ended.extend(mine);
        }
    }
    meta.quiesce();
    assert_eq!(meta.in_flight(), 0, "quiesce settles every in-flight batch");
    let held_and_landed = assert_invariants(&meta, &ended).await;
    Swept {
        observations: meta.observations(),
        ended,
        raced,
        held_and_landed,
    }
}

/// The four invariants over the store after `quiesce`, against how every Abort ended. Returns how
/// many Aborts ended saying a fence may still land whose session is now `Aborting`.
async fn assert_invariants(meta: &SimFdbMetadataStore, ended: &[Ended]) -> usize {
    let mut sessions: BTreeMap<String, SessionRecord> = BTreeMap::new();
    for (key, value) in meta.scan(MPU_PREFIX).await.unwrap() {
        let id = parse_mpu_key(&key).expect("a canonical mpu: key");
        let record = decode_session_record(&value).expect("a valid session record");
        sessions.insert(id.as_str().to_owned(), record);
    }
    let count = match meta.get(MPUCTL_KEY).await.unwrap() {
        Some(bytes) => decode_admission_record(&bytes)
            .expect("a valid ledger")
            .count(),
        None => 0,
    };

    // (1) the ledger counts every session, fenced or not.
    assert_eq!(
        sessions.len() as u64,
        count,
        "the number of mpu: records must equal mpuctl.count()"
    );

    // (2) every Aborting session has its {session, all} obligation, and no obligation is
    // without its fenced session.
    let mut owed = Vec::new();
    for (id, record) in &sessions {
        let state = record.state();
        if !matches!(state, SessionState::Aborting {}) {
            assert!(
                matches!(state, SessionState::Open {}),
                "{id}: only Open and Aborting are reachable here, found {state:?}"
            );
            continue;
        }
        let token = RetireToken::Session {
            upload_id: UploadId::new(id.clone()).unwrap(),
            epoch: record.epoch().checked_sub(1).expect("a fenced epoch"),
            part: None,
        };
        let key = retire_key(RetireMode::Bytes, &token);
        let Some(value) = meta.get(&key).await.unwrap() else {
            panic!("{id} is Aborting without its retire:bytes: obligation");
        };
        let (mode, decoded, payload) =
            decode_retire_obligation(&key, &value).expect("the obligation decodes at its key");
        assert_eq!((mode, decoded), (RetireMode::Bytes, token), "{id}");
        assert!(
            payload.session() && payload.parts() == Some(&PartScope::All),
            "{id}: the obligation owes {{session, all}}"
        );
        owed.push(key);
    }
    let mut stored: Vec<Vec<u8>> = Vec::new();
    for prefix in [RETIRE_BYTES_PREFIX, RETIRE_RECORDS_PREFIX] {
        stored.extend(meta.scan(prefix).await.unwrap().into_iter().map(|(k, _)| k));
    }
    stored.sort();
    owed.sort();
    assert_eq!(stored, owed, "no obligation is without its fenced session");

    // (4) every ending is one this scenario can reach.
    for end in ended {
        let id = &end.upload_id;
        match &end.outcome {
            Ok(AbortOutcome::Fenced | AbortOutcome::AlreadyAborting) => {}
            Ok(AbortOutcome::Refused(Refusal::NoSuchUpload)) => {}
            Err(AbortError::InDoubt(doubt)) => assert!(
                doubt.may_still_commit,
                "{id}: an out-of-flight outcome left unsettled, though every read succeeds: \
                 {doubt:?}"
            ),
            Err(AbortError::Stopped {
                cause: Stop::RoundsSpent { .. },
                ..
            }) => {}
            other => panic!("{id}: an Abort ended in a way this scenario cannot reach: {other:?}"),
        }
    }

    // (3) a session is Aborting exactly when an Abort of it said it is, or may be.
    let mut by_session: BTreeMap<&str, Vec<&Ended>> = BTreeMap::new();
    for end in ended {
        by_session
            .entry(end.upload_id.as_str())
            .or_default()
            .push(end);
    }
    let mut held_and_landed = 0;
    for (id, ends) in &by_session {
        let Some(record) = sessions.get(*id) else {
            // A create still in doubt whose batch never landed: no Abort can have fenced it.
            assert!(
                ends.iter().all(|end| !end.reported_fence()),
                "{id}: an Abort reported a fence of a session that does not exist"
            );
            continue;
        };
        let aborting = matches!(record.state(), SessionState::Aborting {});
        for end in ends {
            if end.reported_fence() {
                assert!(
                    aborting,
                    "{id}: an Abort answered {:?}, but the session is {:?}",
                    end.outcome,
                    record.state()
                );
            }
            if end.may_still_land() && aborting {
                held_and_landed += 1;
            }
        }
        let reported = ends
            .iter()
            .any(|end| end.reported_fence() || end.may_still_land());
        if !reported {
            assert!(
                matches!(record.state(), SessionState::Open {}),
                "session {id} is {:?}, not Open: no Abort of it answered Fenced or \
                 AlreadyAborting, or ended saying a fence may still land",
                record.state()
            );
        }
    }
    held_and_landed
}

/// One seed of `leg`, start to finish.
fn run(seed: u64, leg: Leg, report: Report) -> Swept {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
            FdbFidelity::CommitUnknownResult,
        ));
        abort_under_ambiguity(meta, leg, report).await
    })
}

/// A sweep of `leg` over [`SWEEP_SEEDS`]: every invariant at every seed, the nemesis striking
/// at every seed, and at least one Abort that spent every round. Returns the swept seeds.
fn sweep(leg: Leg) -> Vec<Swept> {
    let swept: Vec<Swept> = (0..SWEEP_SEEDS)
        .map(|seed| run(seed, leg, Report::Faithful))
        .collect();
    for (seed, one) in swept.iter().enumerate() {
        assert!(
            one.observations.ambiguous_commits >= 1,
            "{leg:?}: the nemesis struck at seed {seed}"
        );
    }
    let spent = swept
        .iter()
        .flat_map(|one| &one.ended)
        .filter(|end| end.spent_every_round())
        .count();
    assert!(
        spent >= 1,
        "vacuous: no Abort in {leg:?} 0..{SWEEP_SEEDS} spent all {ABORT_ROUNDS} rounds"
    );
    swept
}

/// **The seed sweep `cargo xtask dst` drives** (`MADSIM_TEST_NUM=50`): all four legs, each on
/// its own store, every invariant at every seed.
#[madsim::test]
async fn multipart_abort_invariants_hold_under_the_dst_seed_sweep() {
    for leg in [
        Leg::Unknown1021,
        Leg::TimedOut1031,
        Leg::Mixed,
        Leg::Witness,
    ] {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
            FdbFidelity::CommitUnknownResult,
        ));
        let swept = abort_under_ambiguity(meta, leg, Report::Faithful).await;
        assert!(
            swept.observations.ambiguous_commits >= 1,
            "{leg:?}: the nemesis must strike — a sweep that never fires it is vacuous"
        );
        assert!(!swept.ended.is_empty(), "{leg:?}: some Abort ran");
    }
}

/// **1021.** Every invariant holds at every seed, two Aborts raced for a session, and some Abort
/// spent every round with the session still `Open`.
#[test]
fn the_1021_leg_holds_and_some_aborts_spend_every_round() {
    let swept = sweep(Leg::Unknown1021);
    assert!(swept.iter().any(|one| one.raced >= 1), "two Aborts raced");
}

/// **1031.** Every invariant holds at every seed, including the seeds where a fence was left in
/// flight and landed, or was rejected, after its Abort had returned.
#[test]
fn the_1031_leg_holds_and_some_aborts_spend_every_round() {
    let swept = sweep(Leg::TimedOut1031);
    let in_flight: u64 = swept
        .iter()
        .map(|one| one.observations.commits_left_in_flight)
        .sum();
    assert!(in_flight >= 1, "no 1031 in the sweep was left in flight");
}

/// **Mixed.** Every invariant holds at every seed, and at least one Abort met a 1031 and then a
/// 1021: the codes one invocation met, in order, include both.
#[test]
fn the_mixed_leg_holds_and_one_abort_meets_1031_then_1021() {
    let swept = sweep(Leg::Mixed);
    let witnessed = swept
        .iter()
        .flat_map(|one| &one.ended)
        .filter(|end| end.met_1031_then_1021())
        .count();
    assert!(
        witnessed >= 1,
        "no Abort in 0..{SWEEP_SEEDS} met a 1031 and then a 1021"
    );
}

/// **Witness: a kept doubt whose fence lands.** Every invariant holds at every seed, and across
/// the sweep at least one Abort ended saying a fence may still land, and that fence landed by
/// `quiesce`: the session is `Aborting` with its obligation, and the error named it.
#[test]
fn the_witness_leg_holds_and_a_held_fence_lands_after_its_abort_returned() {
    let swept = sweep(Leg::Witness);
    let landed: usize = swept.iter().map(|one| one.held_and_landed).sum();
    assert!(
        landed >= 1,
        "vacuous: no Abort in 0..{SWEEP_SEEDS} ended in doubt with a fence that then landed"
    );
}

/// **Demonstrated red — invariant (3) is load-bearing.** The wrapper reports each struck 1031 as
/// `Conflict`. An Abort whose fences all stay in flight finds the session unchanged and the key
/// absent each time, spends its rounds, and says nothing is in doubt; its fence then lands at
/// `quiesce`, leaving a fenced session no Abort reported. If no seed reached that, this would not
/// panic and `should_panic` would fail it.
#[test]
#[should_panic(expected = "not Open")]
fn a_1031_misreported_as_conflict_leaves_a_fence_no_abort_reported() {
    for seed in 0..SWEEP_SEEDS {
        run(seed, Leg::Witness, Report::TimedOut1031AsConflict);
    }
}
