//! **Multipart admission under commit ambiguity** — concurrent `CreateMultipartUpload`s
//! (`wyrd_core::multipart::create::create_session`, #894) on the simulated-FoundationDB store
//! with the commit-ambiguity nemesis armed, inside the deterministic simulator. A new concurrent
//! path lands with seeded Tier-0 DST coverage (`AGENTS.md`, review rubric, *Test fidelity*).
//!
//! Three legs race six creators for a ledger with room for four, so admission, the capacity
//! refusal and the contention budget all run while commits come back ambiguous, one leg per
//! shape of the ambiguity class (`crates/metadata-fdb/src/lib.rs:159-166`). A fourth leg runs
//! one creator alone:
//!
//! * **1021 alone** — the transaction is out of flight; a re-read settles it. The sweep must see
//!   at least one 1021 that did **not** apply, read from the simulator's own counters, or the
//!   reverse assertion below was never tested and the sweep is vacuous.
//! * **1031 alone** — the batch may land after the create's settling read, or later still, at
//!   [`SimFdbMetadataStore::quiesce`].
//! * **mixed** — each strike's code is a seed-derived coin, so one writer can meet a 1031 and
//!   then a 1021. The sweep must see that happen at least once.
//! * **witness** — one creator, a contention budget of [`WITNESS_ROOM`] units, and 1031 armed
//!   for more commits than that, on a store whose in-flight batches land only at `quiesce`
//!   ([`SimFdbMetadataStore::defer_in_flight_landings`]). With no other writer to move its
//!   preconditions, a batch left in flight then lands, through the resolver, after the create
//!   gave up. The sweep must see at least one create that ended **in doubt with
//!   `may_still_commit`** and whose batch then landed: the session it named is exactly its
//!   record, and counted. That is the case the in-doubt error exists for. The racing legs almost
//!   never reach it, because other creators move `mpuctl` under the in-flight batch, and without
//!   the deferral the batch lands opportunistically before the create's settling read.
//!
//! After every seed, on the store after `quiesce`:
//!
//! 1. the number of `mpu:` records equals `mpuctl.count()`;
//! 2. every `mpu:` record has its `seggrp:` marker, and no marker is without its session;
//! 3. every `mpu:` record belongs to a create that answered `Created` for it, or to one whose
//!    in-doubt error named that upload id and nonce, and is then exactly the record that create's
//!    batch puts — none is stranded;
//! 4. the reverse: **every `Created` is real** — its `mpu:<id>` exists and decodes as an `Open`
//!    session for the requested bucket and object, its `seggrp:<nonce>` marker exists, and it is
//!    one of the records `mpuctl.count()` counts.
//!
//! Each direction ships its **demonstrated red**: a store wrapper that misreports to the create
//! what the simulator did. Told `Committed` for a struck 1021, a create answers `Created` for a
//! batch that may never have applied, and (4) catches it. Told `Conflict` for a struck 1031, a
//! create that later finds its own session cannot tell it from a collision and re-mints, leaving
//! that session stranded, and (3) catches it.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`, which sweeps 50 seeds over the
//! `#[madsim::test]` entry point); without it this file compiles to nothing.

#![forbid(unsafe_code)]
#![cfg(madsim)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self, InodeId, SegmentNonce};
use wyrd_core::multipart::create::{create_session, CreateError, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    decode_admission_record, decode_session_record, parse_mpu_key, Budget, CreateOutcome,
    SessionRecord, SessionState, UploadId, MPUCTL_KEY, MPU_PREFIX,
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
/// Creators per seed in the racing legs, racing for [`ROOM`] places.
const CREATORS: usize = 6;
/// The racing legs' ledger room: a profile admitting exactly this many sessions.
const ROOM: u64 = 4;
/// Commits the nemesis strikes per seed in the racing legs.
const STRIKES: u64 = 8;
/// The witness leg's ledger room, and so its lone creator's contention budget.
const WITNESS_ROOM: u64 = 3;
/// Commits the witness leg's nemesis strikes: more than its creator's whole budget.
const WITNESS_STRIKES: u64 = 8;
/// Seeds each plain-`#[test]` sweep drives; they must see every fate of a struck commit.
const SWEEP_SEEDS: u64 = 64;

/// Which shape of the ambiguity class a leg arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Unknown1021,
    TimedOut1031,
    Mixed,
    /// One creator, 1031 armed past its whole contention budget, and every batch left in flight
    /// landing only at `quiesce` ([`SimFdbMetadataStore::defer_in_flight_landings`]). Without
    /// that, each later store call lands an in-flight batch on a seed coin, so it lands before
    /// the create's settling read on all but about one seed in a hundred at a budget of one unit,
    /// and on none of 1024 seeds at three.
    Witness,
}

/// What the store wrapper tells the create about a struck commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Report {
    /// The truth: the simulator's own `CommitUnknownResult`.
    Faithful,
    /// **Violating**: a struck 1021 reported as `Committed`, whether or not it applied.
    Unknown1021AsCommitted,
    /// **Violating**: a struck 1031 reported as `Conflict` — what the trait forbids, since
    /// `Conflict` asserts nothing was written.
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

/// One creator's store: the shared simulator, with the codes of the struck commits this creator
/// met recorded in order, and reported to it as [`Report`] says.
struct Creator {
    inner: Arc<SimFdbMetadataStore>,
    report: Report,
    codes: Mutex<Vec<i32>>,
}

#[async_trait]
impl MetadataStore for Creator {
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
            (Report::Unknown1021AsCommitted, SIM_COMMIT_UNKNOWN_RESULT) => {
                Ok(CommitOutcome::Committed)
            }
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

/// How one creator ended.
struct Ended {
    object: String,
    outcome: Result<CreateOutcome, CreateError>,
    /// The codes of the struck commits it met, in order.
    codes: Vec<i32>,
}

impl Ended {
    /// The upload id this create told its caller about — by `Created`, or by an in-doubt error,
    /// which also names the nonce.
    fn claim(&self) -> Option<(&UploadId, Option<&SegmentNonce>)> {
        match &self.outcome {
            Ok(CreateOutcome::Created { upload_id }) => Some((upload_id, None)),
            Err(CreateError::InDoubt(doubt)) => {
                Some((&doubt.upload_id, Some(&doubt.segment_nonce)))
            }
            // Ends that tell the caller about no session; invariant (3) checks none was left.
            Ok(CreateOutcome::Refused(_)) | Err(CreateError::IdentityCollisions { .. }) => None,
            Err(other) => panic!(
                "{}: an unexpected end in the simulator: {other}",
                self.object
            ),
        }
    }

    /// Whether this creator ended in doubt with a batch that may still land.
    fn held_in_doubt(&self) -> bool {
        matches!(&self.outcome, Err(CreateError::InDoubt(doubt)) if doubt.may_still_commit)
    }

    /// Whether this creator met a 1031 and, later, a 1021.
    fn met_1031_then_1021(&self) -> bool {
        self.codes
            .iter()
            .position(|code| *code == SIM_TRANSACTION_TIMED_OUT)
            .is_some_and(|first| self.codes[first..].contains(&SIM_COMMIT_UNKNOWN_RESULT))
    }
}

/// What one seed of a leg left behind.
struct Swept {
    observations: FdbObservations,
    ended: Vec<Ended>,
    /// Creates that ended in doubt with `may_still_commit` and whose batch landed by `quiesce`:
    /// the session they named is exactly their record, and counted (invariant (3) checked it).
    held_in_doubt_and_landed: usize,
}

/// The scenario body: the `leg`'s creators with its nemesis armed, then `quiesce`, then the
/// four invariants.
async fn admission_under_ambiguity(
    meta: Arc<SimFdbMetadataStore>,
    leg: Leg,
    report: Report,
) -> Swept {
    // The fixture's commit is made with the nemesis disarmed.
    let marker = WriteBatch::new().put(metadata::bucket_key(BUCKET), MARKER);
    meta.commit(marker).await.unwrap();
    let (creators, room) = match leg {
        Leg::Witness => (1, WITNESS_ROOM),
        _ => (CREATORS, ROOM),
    };
    match leg {
        Leg::Unknown1021 => meta.arm_commit_ambiguity(SIM_COMMIT_UNKNOWN_RESULT, STRIKES),
        Leg::TimedOut1031 => meta.arm_commit_ambiguity(SIM_TRANSACTION_TIMED_OUT, STRIKES),
        Leg::Mixed => meta.arm_mixed_commit_ambiguity(STRIKES),
        Leg::Witness => {
            meta.defer_in_flight_landings();
            meta.arm_commit_ambiguity(SIM_TRANSACTION_TIMED_OUT, WITNESS_STRIKES);
        }
    }
    let seed = madsim::runtime::Handle::current().seed();
    let mut tasks = Vec::new();
    for i in 0..creators {
        let store = Creator {
            inner: Arc::clone(&meta),
            report,
            codes: Mutex::new(Vec::new()),
        };
        let mut runtime = SimRuntime {
            rng: rand::SeedableRng::seed_from_u64(seed ^ (0x9e37_79b9 * (i as u64 + 1))),
        };
        tasks.push(madsim::task::spawn(async move {
            let object = format!("{BUCKET}/object-{i}");
            let request = CreateRequest {
                bucket: BUCKET,
                parent: ROOT,
                object: &object,
                content_type: None,
                created_at_millis: CREATED_AT,
                clock_source: CLOCK,
            };
            let outcome = create_session(&store, &profile_of(room), &request, &mut runtime).await;
            let codes = store.codes.into_inner().unwrap();
            Ended {
                object,
                outcome,
                codes,
            }
        }));
    }
    let mut ended = Vec::new();
    for task in tasks {
        ended.push(task.await.unwrap());
    }
    meta.quiesce();
    assert_eq!(meta.in_flight(), 0, "quiesce settles every in-flight batch");
    let held_in_doubt_and_landed = assert_invariants(&meta, &ended, room).await;
    Swept {
        observations: meta.observations(),
        ended,
        held_in_doubt_and_landed,
    }
}

/// Whether `bytes` is exactly the record `end`'s batch puts under nonce `nonce`: the session
/// Create installs for its request (`Open` at epoch 0, no fence spent, the request's own object,
/// clock and stamp, no content type), in the encoder's canonical spelling.
fn is_record_of(bytes: &Bytes, record: &SessionRecord, end: &Ended, nonce: &SegmentNonce) -> bool {
    matches!(record.state(), SessionState::Open {})
        && record.parent() == ROOT
        && record.object() == end.object
        && record.content_type().is_none()
        && record.created_at_millis() == CREATED_AT
        && record.clock_source() == CLOCK
        && record.segment_nonce() == nonce
        && record.epoch() == 0
        && record.attempts() == 0
        && metadata::encode(record) == *bytes
}

/// The four invariants over the store after `quiesce`, against what every create answered.
/// Returns how many creates ended held in doubt and had their batch land.
async fn assert_invariants(meta: &SimFdbMetadataStore, ended: &[Ended], room: u64) -> usize {
    let mut sessions: BTreeMap<String, (Bytes, SessionRecord)> = BTreeMap::new();
    for (key, value) in meta.scan(MPU_PREFIX).await.unwrap() {
        let id = parse_mpu_key(&key).expect("a canonical mpu: key");
        let record = decode_session_record(&value).expect("a valid session record");
        sessions.insert(id.as_str().to_owned(), (value, record));
    }
    let count = match meta.get(MPUCTL_KEY).await.unwrap() {
        Some(bytes) => decode_admission_record(&bytes)
            .expect("a valid ledger")
            .count(),
        None => 0,
    };

    // (1) the ledger counts every session.
    assert_eq!(
        sessions.len() as u64,
        count,
        "the number of mpu: records must equal mpuctl.count()"
    );
    assert!(
        count <= room,
        "the ledger never admits past its stored limit"
    );

    // (2) every session has its marker, and no marker is without its session.
    let markers: BTreeSet<Vec<u8>> = meta
        .scan(metadata::SEGGRP_PREFIX)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    let expected: BTreeSet<Vec<u8>> = sessions
        .values()
        .map(|(_, record)| metadata::seggrp_key(record.segment_nonce()))
        .collect();
    assert_eq!(
        markers, expected,
        "every mpu: record has its seggrp: marker, and no marker is without its session"
    );

    // (3) no session is stranded: each is one its create told its caller about.
    let mut claims: BTreeMap<String, (&Ended, Option<&SegmentNonce>)> = BTreeMap::new();
    for end in ended {
        if let Some((id, nonce)) = end.claim() {
            let previous = claims.insert(id.as_str().to_owned(), (end, nonce));
            assert!(previous.is_none(), "two creates claimed upload {id}");
        }
    }
    let mut held_in_doubt_and_landed = 0;
    for (id, (bytes, record)) in &sessions {
        let Some((end, nonce)) = claims.get(id) else {
            panic!(
                "session {id} ({}) is stranded: no create answered Created for it or named it \
                 in an in-doubt error",
                record.object()
            );
        };
        assert_eq!(
            record.object(),
            end.object,
            "{id} is the session its create asked for"
        );
        if let Some(nonce) = nonce {
            assert!(
                is_record_of(bytes, record, end, nonce),
                "{id}: the session an in-doubt error named is exactly that create's record, \
                 under the nonce it named"
            );
            if end.held_in_doubt() {
                held_in_doubt_and_landed += 1;
            }
        }
    }

    // (4) the reverse: every Created is a counted, stored session.
    for end in ended {
        let Ok(CreateOutcome::Created { upload_id }) = &end.outcome else {
            continue;
        };
        let Some((bytes, record)) = sessions.get(upload_id.as_str()) else {
            panic!(
                "{} answered Created for {upload_id}, but no such session exists — a Created \
                 that is not real",
                end.object
            );
        };
        assert!(
            is_record_of(bytes, record, end, record.segment_nonce()),
            "{upload_id}: created Open, for the requested bucket and object"
        );
        assert!(markers.contains(&metadata::seggrp_key(record.segment_nonce())));
    }
    held_in_doubt_and_landed
}

/// One seed of `leg`, start to finish.
fn run(seed: u64, leg: Leg, report: Report) -> Swept {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
            FdbFidelity::CommitUnknownResult,
        ));
        admission_under_ambiguity(meta, leg, report).await
    })
}

/// **The seed sweep `cargo xtask dst` drives** (`MADSIM_TEST_NUM=50`): all four legs, each on
/// its own store, every invariant at every seed.
#[madsim::test]
async fn multipart_admission_invariants_hold_under_the_dst_seed_sweep() {
    for leg in [
        Leg::Unknown1021,
        Leg::TimedOut1031,
        Leg::Mixed,
        Leg::Witness,
    ] {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
            FdbFidelity::CommitUnknownResult,
        ));
        let swept = admission_under_ambiguity(meta, leg, Report::Faithful).await;
        assert!(
            swept.observations.ambiguous_commits >= 1,
            "{leg:?}: the nemesis must strike — a sweep that never fires it is vacuous"
        );
    }
}

/// **1021 alone.** Every invariant holds at every seed, and the sweep sees at least one 1021
/// that did **not** apply — the case where a create that answered `Created` without settling
/// would leave no session, which only the reverse assertion can catch.
#[test]
fn the_1021_leg_holds_and_meets_a_1021_that_did_not_apply() {
    let mut unapplied = 0u64;
    for seed in 0..SWEEP_SEEDS {
        let obs = run(seed, Leg::Unknown1021, Report::Faithful).observations;
        assert!(
            obs.ambiguous_commits >= 1,
            "the nemesis struck at seed {seed}"
        );
        assert_eq!(
            obs.commits_left_in_flight, 0,
            "1021 is out of flight (seed {seed})"
        );
        if obs.ambiguous_commits > obs.ambiguous_commits_that_landed {
            unapplied += 1;
        }
    }
    assert!(
        unapplied >= 1,
        "vacuous: every 1021 in 0..{SWEEP_SEEDS} landed, so the reverse assertion was never \
         tested against a 1021 that did not apply"
    );
}

/// **1031 alone.** Every invariant holds at every seed, including the seeds where a struck
/// batch was left in flight and landed — or was rejected — after its create had returned.
#[test]
fn the_1031_leg_holds_through_late_landings() {
    let (mut in_flight, mut late) = (0u64, 0u64);
    for seed in 0..SWEEP_SEEDS {
        let obs = run(seed, Leg::TimedOut1031, Report::Faithful).observations;
        assert!(
            obs.ambiguous_commits >= 1,
            "the nemesis struck at seed {seed}"
        );
        in_flight += obs.commits_left_in_flight;
        late += obs.deferred_landings + obs.deferred_rejections;
    }
    assert!(in_flight >= 1, "no 1031 in the sweep was left in flight");
    assert!(late >= 1, "no in-flight 1031 in the sweep was settled late");
}

/// **Mixed.** Every invariant holds at every seed, and across the sweep at least one writer met
/// a 1031 and then a 1021: the struck codes one writer met, in order, include both.
#[test]
fn the_mixed_leg_holds_and_one_writer_meets_1031_then_1021() {
    let mut witnessed = 0usize;
    for seed in 0..SWEEP_SEEDS {
        let swept = run(seed, Leg::Mixed, Report::Faithful);
        witnessed += swept
            .ended
            .iter()
            .filter(|end| end.met_1031_then_1021())
            .count();
    }
    assert!(
        witnessed >= 1,
        "no writer in 0..{SWEEP_SEEDS} met a 1031 and then a 1021"
    );
}

/// **Witness: a kept doubt whose batch lands.** Every invariant holds at every seed, and across
/// the sweep at least one create ended `InDoubt { may_still_commit: true }` and then had its
/// batch land by `quiesce`: its `mpu:<id>` is exactly that create's record, under the nonce the
/// error named, and `mpuctl` counts it. A sweep that never meets this has not tested that an
/// in-doubt error names the session that lands.
#[test]
fn the_witness_leg_holds_and_a_held_in_doubt_batch_lands_by_quiesce() {
    let (mut held, mut landed) = (0usize, 0usize);
    for seed in 0..SWEEP_SEEDS {
        let swept = run(seed, Leg::Witness, Report::Faithful);
        assert!(
            swept.observations.ambiguous_commits >= 1,
            "the nemesis struck at seed {seed}"
        );
        held += swept.ended.iter().filter(|end| end.held_in_doubt()).count();
        landed += swept.held_in_doubt_and_landed;
    }
    assert!(
        held >= 1,
        "vacuous: no create in 0..{SWEEP_SEEDS} ended in doubt with may_still_commit"
    );
    assert!(
        landed >= 1,
        "vacuous: of {held} creates that ended in doubt with may_still_commit in \
         0..{SWEEP_SEEDS}, none had its batch land by quiesce, so no sweep tested that the \
         in-doubt error names the session that lands"
    );
}

/// **Demonstrated red — the reverse assertion is load-bearing.** The wrapper reports every
/// struck 1021 to the create as `Committed`, so the create answers `Created` for a batch it
/// never settled by a read. On a seed where that 1021 did not apply, no session exists, and
/// invariant (4) fails. If no seed had an unapplied 1021, this would not panic and
/// `should_panic` would fail it.
#[test]
#[should_panic(expected = "a Created that is not real")]
fn a_created_for_an_unsettled_1021_fails_the_reverse_assertion() {
    for seed in 0..SWEEP_SEEDS {
        run(seed, Leg::Unknown1021, Report::Unknown1021AsCommitted);
    }
}

/// **Demonstrated red — "no session is stranded" is load-bearing.** The wrapper reports every
/// struck 1031 as `Conflict`. The create then has no evidence that a session it later finds
/// under its identity is its own, reads it as a collision and re-mints, and the session the
/// 1031 batch installed is one no caller was told about: invariant (3) fails.
#[test]
#[should_panic(expected = "is stranded")]
fn a_1031_misreported_as_conflict_strands_a_session() {
    for seed in 0..SWEEP_SEEDS {
        run(seed, Leg::TimedOut1031, Report::TimedOut1031AsConflict);
    }
}
