//! The concurrent consistency-workload substrate (issue #406, ADR-0041 §Decision, #329
//! slice 3): builds on the landed #405 observable
//! ([`wyrd_server::consistency_observable`]) a **multi-process** register history, an
//! **Elle-EDN serializer**, **session** read-your-writes / monotonic-read checks, a
//! **directory-as-set** history, a **concurrency witness**, and an off-Check
//! **verdict-dispatch** seam — while keeping the linearizability *verdict* itself off-Check
//! (ADR-0041/ADR-0016: no JVM/Clojure in `cargo xtask ci`).
//!
//! # Two soundness invariants, pinned SURFACE-WIDE (the reason #406 was re-planned)
//!
//! Every prior attempt fixed the two governing faults only at the one spot a sign-off named,
//! and the same fault-class reappeared in an adjacent arm. This file ships one crafted,
//! **socket-free**, flippable red per audited arm so the CLASS goes red if it reappears
//! anywhere.
//!
//! **INV-1 (no fabricated certainty).** An **indeterminate** wire outcome (5xx / timeout /
//! synthetic-0) must never become a **definite** obligation, completion type, or membership.
//! Audited: the register serializer completion-type; the directory serializer completion-type
//! and its membership derivation; the RYW **PUT** arm and **DELETE** arm obligation
//! establishment; the RYW read side; and both monotonic-read checks.
//!
//! **INV-2 (non-vacuity).** An overlap counts as genuine concurrency ONLY when it constrains a
//! single register — **same-key**, **read↔write**, across **distinct processes**. A cross-key
//! overlap and a read↔read overlap are vacuous and must NOT count.
//!
//! The **core reds are socket-free**: crafted histories fed through the pure serializer / session
//! checks / witness / dispatch, asserted on real inputs (RED on any module weakening). The
//! **wire-driven concurrent workload** (leg a's positive witness) binds `127.0.0.1:0` and drives
//! real signed HTTP, exactly as #405's landed test does; its green is confirmed by the full
//! `cargo xtask ci` (which permits loopback bind). The linearizability verdict is Elle's,
//! off-Check, over the SAME serialized history this file produces.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::Credentials;
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::consistency_observable::{ObservableS3Client, OpKind, OpRecord};
use wyrd_server::consistency_workload::{
    consistency_verdict_dispatch, membership, ConsistencyVerdictDispatch, DirOpKind, DirRecord,
    DirectoryHistory, Membership, MultiProcessHistory, ProcOp, ELLE_IN_GATE_CMD_VAR,
    ELLE_OFF_CHECK_VERDICT_JOB,
};
use wyrd_server::Gateway;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "wyrd-bucket";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

// ─── Crafted-history helpers (socket-free) ────────────────────────────────────────────

/// A `SystemTime` `nanos` after the Unix epoch — so crafted real-time spans serialize to
/// small, readable relative-nanosecond `:time` values.
fn at(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

fn rec(
    kind: OpKind,
    key: &str,
    version: Option<u64>,
    status: u16,
    start: u64,
    end: u64,
) -> OpRecord {
    OpRecord {
        kind,
        key: key.to_string(),
        version,
        status,
        start: at(start),
        end: at(end),
    }
}

fn proc_op(
    process: usize,
    kind: OpKind,
    key: &str,
    version: Option<u64>,
    status: u16,
    start: u64,
    end: u64,
) -> ProcOp {
    ProcOp {
        process,
        record: rec(kind, key, version, status, start, end),
    }
}

fn dir_rec(op: DirOpKind, member: &str, status: u16, start: u64, end: u64) -> DirRecord {
    DirRecord {
        process: 0,
        op,
        member: member.to_string(),
        status,
        start: at(start),
        end: at(end),
    }
}

// ─── (b) Elle-EDN serializer: byte-exact golden, indeterminate → :info (INV-1) ─────────

#[test]
fn register_serializer_emits_byte_exact_elle_edn_with_info_for_indeterminate() {
    // A crafted 2-process register history: P0 overwrites the shared key (v1 determinate, v2
    // INDETERMINATE via a 500), P1 reads it (v1 determinate-present, then a determinate 404
    // absent). The indeterminate PUT must serialize `:info` (never a definite `:ok`), and the
    // determinate 404 read must serialize a definite `:ok` of `nil` — proving the completion-type
    // arm distinguishes indeterminate from determinate-absent.
    let history = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(1, OpKind::Get, "k", Some(1), 200, 5, 15),
        proc_op(0, OpKind::Put, "k", Some(2), 500, 30, 40),
        proc_op(1, OpKind::Get, "k", None, 404, 50, 60),
    ]);

    // The exact bytes (an EDN operation-history vector, one map per line). Written as a literal,
    // NOT recomputed with the serializer's own join, so a delimiter/field/order change is caught.
    let expected = concat!(
        "[{:process 0, :type :invoke, :f :write, :value 1, :time 0}\n",
        " {:process 1, :type :invoke, :f :read, :value nil, :time 5}\n",
        " {:process 0, :type :ok, :f :write, :value 1, :time 10}\n",
        " {:process 1, :type :ok, :f :read, :value 1, :time 15}\n",
        " {:process 0, :type :invoke, :f :write, :value 2, :time 30}\n",
        " {:process 0, :type :info, :f :write, :value 2, :time 40}\n",
        " {:process 1, :type :invoke, :f :read, :value nil, :time 50}\n",
        " {:process 1, :type :ok, :f :read, :value nil, :time 60}]",
    );

    assert_eq!(
        history.to_elle_edn(),
        expected,
        "the register serializer must emit byte-exact Elle EDN, mapping the indeterminate PUT \
         to :info (never a definite :ok) and the determinate 404 read to a definite :ok of nil"
    );
    // Guard rail on the load-bearing INV-1 byte: the indeterminate write is `:info`, and no
    // definite `:ok` is fabricated for it.
    assert!(
        history
            .to_elle_edn()
            .contains(":type :info, :f :write, :value 2"),
        "an indeterminate write must carry :info, not a fabricated definite outcome"
    );
}

// ─── (d) Directory-as-set serializer: indeterminate probe → :info, no fabricated false ─

#[test]
fn directory_serializer_maps_indeterminate_probe_to_info_not_fabricated_absence() {
    // create=PUT / delete=DELETE / membership=GET-probe (200 present / 404 absent). The probe
    // of "bob" times out (503, indeterminate) — it must serialize `:info` with `["bob" nil]`,
    // NEVER a fabricated `["bob" false]`.
    let history = DirectoryHistory::from_records(vec![
        dir_rec(DirOpKind::Create, "alice", 200, 0, 10),
        dir_rec(DirOpKind::Probe, "alice", 200, 20, 30),
        dir_rec(DirOpKind::Delete, "alice", 204, 40, 50),
        dir_rec(DirOpKind::Probe, "alice", 404, 60, 70),
        dir_rec(DirOpKind::Probe, "bob", 503, 80, 90),
    ]);

    let expected = concat!(
        "[{:process 0, :type :invoke, :f :add, :value \"alice\", :time 0}\n",
        " {:process 0, :type :ok, :f :add, :value \"alice\", :time 10}\n",
        " {:process 0, :type :invoke, :f :contains, :value [\"alice\" nil], :time 20}\n",
        " {:process 0, :type :ok, :f :contains, :value [\"alice\" true], :time 30}\n",
        " {:process 0, :type :invoke, :f :remove, :value \"alice\", :time 40}\n",
        " {:process 0, :type :ok, :f :remove, :value \"alice\", :time 50}\n",
        " {:process 0, :type :invoke, :f :contains, :value [\"alice\" nil], :time 60}\n",
        " {:process 0, :type :ok, :f :contains, :value [\"alice\" false], :time 70}\n",
        " {:process 0, :type :invoke, :f :contains, :value [\"bob\" nil], :time 80}\n",
        " {:process 0, :type :info, :f :contains, :value [\"bob\" nil], :time 90}]",
    );

    assert_eq!(
        history.to_elle_edn(),
        expected,
        "the directory serializer must map the indeterminate probe to :info with [member nil], \
         never a fabricated [member false]"
    );
    assert!(
        !history.to_elle_edn().contains("[\"bob\" false]"),
        "an indeterminate membership probe must never fabricate a definite [member false]"
    );
}

// ─── (d) Membership derivation: 200→present, 404→absent, else→unknown (INV-1 v) ────────

#[test]
fn membership_derivation_never_coerces_unknown_to_absent() {
    assert_eq!(membership(200), Membership::Present);
    assert_eq!(membership(404), Membership::Absent);
    // Everything else — crucially every indeterminate status — is Unknown, never Absent.
    assert_eq!(membership(503), Membership::Unknown);
    assert_eq!(membership(0), Membership::Unknown);
    assert_eq!(membership(403), Membership::Unknown);
    assert_ne!(
        membership(503),
        Membership::Absent,
        "an indeterminate probe must never be coerced to a definite absence"
    );
}

// ─── (c) Session read-your-writes: sound across EVERY arm (INV-1) ──────────────────────

#[test]
fn session_read_your_writes_guards_indeterminate_on_every_arm() {
    // PUT arm (INV-1 ii-a): with NO prior obligation, an indeterminate write must not ESTABLISH a
    // definite AtLeast from nothing — so reading v=1 afterward is valid. (Pins the "establish"
    // direction only; the "clear a standing bound" direction is pinned by put_arm_clears below.)
    let put_arm = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(2), 500, 0, 10),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 20, 30),
    ]);
    assert!(
        put_arm.session_read_your_writes(),
        "an indeterminate PUT must not create a definite AtLeast obligation from nothing"
    );

    // PUT arm (INV-1 ii-b): an indeterminate write must CLEAR a STANDING AtLeast to Unknown — after
    // a determinate v=5 (AtLeast(5)) an indeterminate v=2 write means the committed version is no
    // longer known, so a later v=1 read is not provably a violation. This is the discriminating red
    // the bare put_arm above misses: drop the `is_indeterminate → Unknown` clear and AtLeast(5)
    // stands, so v=1 < 5 flips this to REJECT. (A determinate-failed 500 also fails is_success, so
    // "does nothing" is NOT a safe substitute for the explicit clear.)
    let put_arm_clears = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(5), 200, 0, 10),
        proc_op(0, OpKind::Put, "k", Some(2), 500, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 40, 50),
    ]);
    assert!(
        put_arm_clears.session_read_your_writes(),
        "an indeterminate PUT must clear a standing AtLeast(5), so a later v=1 read is not a violation"
    );

    // DELETE arm (INV-1 ii-a): the indeterminate delete must not ESTABLISH Absent — with a standing
    // AtLeast(1) from the prior determinate PUT, treating the 500 delete as a success would reject
    // the later v=1 read. Pins the "establish Absent" direction.
    let delete_arm = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Delete, "k", None, 500, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 40, 50),
    ]);
    assert!(
        delete_arm.session_read_your_writes(),
        "an indeterminate DELETE must not create a definite Absent obligation"
    );

    // DELETE arm (INV-1 ii-b): an indeterminate delete must CLEAR a standing AtLeast to Unknown —
    // after a determinate PUT v=1 (AtLeast(1)) an indeterminate delete means the key MIGHT now be
    // absent, so a later determinate 404 read is not provably an own-write-lost. Discriminating red:
    // drop the `is_indeterminate → Unknown` clear and AtLeast(1) stands, so the 404 read trips the
    // own-write-lost branch and flips this to REJECT.
    let delete_arm_clears = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Delete, "k", None, 500, 20, 30),
        proc_op(0, OpKind::Get, "k", None, 404, 40, 50),
    ]);
    assert!(
        delete_arm_clears.session_read_your_writes(),
        "an indeterminate DELETE must clear a standing AtLeast(1), so a later 404 read is not own-write-lost"
    );

    // Read arm (INV-1 iii): an indeterminate GET is no observation — even one crafted to carry a
    // Some(_) version after a determinate delete must not count as a resurrection. RED if the read
    // arm keys on `version.is_some()` instead of the indeterminate status.
    let read_arm = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Delete, "k", None, 204, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(1), 500, 40, 50),
    ]);
    assert!(
        read_arm.session_read_your_writes(),
        "an indeterminate GET (even carrying a stale version) must not be counted a violation"
    );

    // Read arm (INV-1 iii'): a DETERMINATE non-404 failed GET (403) observed nothing about the
    // register — `version` is None only because the status is not 200, NOT because the key is
    // absent. With a standing AtLeast(1) it must NOT be counted an own-write-lost. RED if the read
    // arm keys "definite absence" on `version.is_none()` instead of `status == 404` — the exact
    // INV-1 leak the surface-wide re-plan was meant to close.
    let non_404_failed_read = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Get, "k", None, 403, 20, 30),
    ]);
    assert!(
        non_404_failed_read.session_read_your_writes(),
        "a determinate non-404 failed read (403) observed nothing — it must not fabricate a \
         definite absence / own-write-lost"
    );

    // REJECT — determinate resurrect-after-own-delete: a determinate present read after a
    // determinate DELETE with no intervening PUT is a real RYW violation.
    let resurrect = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Delete, "k", None, 204, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 40, 50),
    ]);
    assert!(
        !resurrect.session_read_your_writes(),
        "a determinate read that resurrects an own-deleted key must be rejected"
    );

    // REJECT — determinate version regression: reading v=1 after determinately writing v=2.
    let regression = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(2), 200, 0, 10),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 20, 30),
    ]);
    assert!(
        !regression.session_read_your_writes(),
        "a determinate read older than one's own determinate write must be rejected"
    );
}

// ─── (c) Session monotonic reads: guard indeterminate, reject determinate regression ───

#[test]
fn session_monotonic_reads_guard_indeterminate_and_reject_determinate_regression() {
    // ACCEPT: the second read is indeterminate (503) though it carries a stale Some(1) version —
    // guarded on status, not on `version == None`, so it is NOT a regression. RED if the guard is
    // dropped.
    let valid_indeterminate = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Get, "k", Some(2), 200, 0, 10),
        proc_op(0, OpKind::Get, "k", Some(1), 503, 20, 30),
    ]);
    assert!(
        valid_indeterminate.session_monotonic_reads(),
        "an indeterminate read must not be counted as a monotonicity regression"
    );

    // REJECT: two determinate reads in one session go backward (2 then 1).
    let regression = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Get, "k", Some(2), 200, 0, 10),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 20, 30),
    ]);
    assert!(
        !regression.session_monotonic_reads(),
        "a determinate in-session read regression must be rejected"
    );
}

// ─── (c) Per-key read monotonicity across processes: sound (INV-1), global ─────────────

#[test]
fn reads_monotone_per_key_is_global_and_guards_indeterminate() {
    // REJECT: a global per-key regression that spans processes — P0 observes v=2, then (later in
    // real time) P1 observes v=1 on the SAME key. This is invisible to the per-session check (each
    // process reads once) but is a genuine stale read across the fleet.
    let global_regression = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Get, "k", Some(2), 200, 0, 10),
        proc_op(1, OpKind::Get, "k", Some(1), 200, 20, 30),
    ]);
    assert!(
        !global_regression.reads_monotone_per_key(),
        "a cross-process per-key read regression must be rejected"
    );
    assert!(
        global_regression.session_monotonic_reads(),
        "the same history is NOT a per-session violation — proving the per-key check is global"
    );

    // ACCEPT: the lower cross-process read is indeterminate (503) → skipped, not a regression.
    let valid_indeterminate = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Get, "k", Some(2), 200, 0, 10),
        proc_op(1, OpKind::Get, "k", Some(1), 503, 20, 30),
    ]);
    assert!(
        valid_indeterminate.reads_monotone_per_key(),
        "an indeterminate read must not be counted as a per-key monotonicity regression"
    );
}

// ─── (a) Concurrency witness: cross-key and read↔read overlaps are vacuous (INV-2) ─────

#[test]
fn concurrency_witness_counts_only_same_key_read_write_overlaps() {
    // NEGATIVE 1 — cross-key only: an overlapping read↔write across processes but on DIFFERENT
    // keys constrains no single register. RED whenever the witness stops requiring same-key (the
    // exact v5 leak).
    let cross_key = MultiProcessHistory::from_ops(vec![
        proc_op(1, OpKind::Get, "a", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Put, "b", Some(1), 200, 5, 15),
    ]);
    assert!(
        !cross_key.is_genuinely_concurrent(),
        "a cross-key read↔write overlap is vacuous — it must not count as genuine concurrency"
    );

    // NEGATIVE 2 — read↔read only: two overlapping reads on the same key across processes impose
    // no ordering constraint on the register.
    let read_read = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Get, "k", Some(1), 200, 0, 10),
        proc_op(1, OpKind::Get, "k", Some(1), 200, 5, 15),
    ]);
    assert!(
        !read_read.is_genuinely_concurrent(),
        "a read↔read overlap is vacuous — it must not count as genuine concurrency"
    );

    // POSITIVE sanity — a same-key read↔write overlap across distinct processes DOES count (so the
    // negatives above are not vacuously false).
    let genuine = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(1, OpKind::Get, "k", Some(1), 200, 5, 15),
    ]);
    assert!(
        genuine.is_genuinely_concurrent(),
        "a same-key read↔write overlap across distinct processes is genuine concurrency"
    );
    assert_eq!(
        genuine.read_write_overlapping_pairs_across_processes(),
        vec![(0, 1)],
        "the witness must identify the single constraining pair"
    );
}

// ─── (e) Verdict-dispatch: routes off-Check by default, never an in-gate JVM shell-out ─

#[test]
fn verdict_dispatch_routes_to_off_check_elle_by_default() {
    // The default input (no in-gate shell-out configured) MUST route to the privileged off-Check
    // Elle job — never a JVM shell-out into `cargo xtask ci`. RED if the default is re-pointed at
    // the in-gate command, exactly the `metadata_faults.rs` shape.
    assert_eq!(
        consistency_verdict_dispatch(false),
        ConsistencyVerdictDispatch::OffCheckElle {
            job: ELLE_OFF_CHECK_VERDICT_JOB
        },
        "the consistency verdict must route to the off-Check Elle job for the default inputs"
    );
    // The in-gate route is representable but reachable ONLY via the deprecated var, and the runner
    // hard-errors rather than shelling a JVM into the gate.
    assert!(
        matches!(
            consistency_verdict_dispatch(true),
            ConsistencyVerdictDispatch::InGateShellOut { env_var }
                if env_var == ELLE_IN_GATE_CMD_VAR
        ),
        "only the deprecated in-gate command var may reach the (never-shelled) in-gate route"
    );
}

// ─── (a) Wire-driven concurrent workload: non-vacuous, genuinely concurrent (leg a) ────

/// Start the S3 gateway on an ephemeral loopback port (mirrors
/// `s3_http_wire.rs::start_gateway` and #405's `consistency_observable.rs` test): the same
/// in-process loopback gateway (redb + fs + mem behind the HTTP listener) that fully exhibits
/// the mutable register, serving connections concurrently (`S3Gateway::serve` → `axum::serve`).
async fn start_gateway() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway: Arc<Backend> = Arc::new(Gateway::new(
        RedbMetadataStore::in_memory().expect("redb"),
        FsChunkStore::open(dir.path()).expect("fs store"),
        MemCoordination::new(),
    ));
    let config = S3Config::new(vec![Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(Arc::clone(&gateway), config);
    tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    (addr, dir)
}

fn client(addr: SocketAddr) -> ObservableS3Client {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    ObservableS3Client::new(addr, BUCKET, creds, REGION)
}

/// Leg (a)'s positive witness: **≥2 concurrent `ObservableS3Client`s** — a sole writer
/// overwriting a shared key and a concurrent reader GETting it — over the real in-process S3
/// HTTP wire produce a **non-vacuous, well-formed, genuinely concurrent** merged multi-process
/// history: ≥1 real-time-overlapping SAME-KEY read↔write span pair across distinct process ids
/// (the only non-vacuous overlap for a single register, INV-2), with the register version
/// climbing across overwrites and per-key reads observing a monotone version sequence.
///
/// Runs on a multi-thread runtime with a start barrier so the two clients genuinely run in
/// parallel; the gateway serves connections concurrently, so their same-key spans overlap in
/// real time. This is the leg whose green is confirmed by the full `cargo xtask ci` (loopback
/// bind permitted); the socket-free reds above carry the flippable RED regardless.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_workload_records_a_nonvacuous_genuinely_concurrent_history() {
    let (addr, _dir) = start_gateway().await;
    const ROUNDS: u64 = 40;
    let key = "shared-register";

    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    // Process 0 — the sole writer, overwriting the shared key v=1..=ROUNDS.
    let writer = {
        let mut c = client(addr);
        let barrier = Arc::clone(&barrier);
        let key = key.to_string();
        tokio::spawn(async move {
            barrier.wait().await;
            for v in 1..=ROUNDS {
                c.put(&key, v).await.expect("put over the wire");
            }
            c.into_history()
        })
    };

    // Process 1 — a concurrent reader, GETting the same key repeatedly.
    let reader = {
        let mut c = client(addr);
        let barrier = Arc::clone(&barrier);
        let key = key.to_string();
        tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..ROUNDS {
                let _ = c.get(&key).await.expect("get over the wire");
            }
            c.into_history()
        })
    };

    let histories = vec![
        writer.await.expect("writer task"),
        reader.await.expect("reader task"),
    ];

    // Merge into one real-time-ordered, `:process`-tagged multi-process history.
    let merged = MultiProcessHistory::merge(&histories);

    // Non-vacuous + well-formed, and every driven op across both clients is recorded.
    assert!(
        merged.well_formed(),
        "the merged multi-process history must be non-vacuous and well-formed"
    );
    assert_eq!(
        merged.ops().len() as u64,
        ROUNDS * 2,
        "every driven op across both concurrent clients must be recorded"
    );

    // INV-2 non-vacuity: ≥1 same-key read↔write overlap across distinct processes — the only
    // overlap that constrains a single register.
    let overlaps = merged.read_write_overlapping_pairs_across_processes();
    assert!(
        !overlaps.is_empty(),
        "a concurrent writer+reader on a shared key must produce ≥1 same-key read↔write real-time \
         overlap across distinct processes (found none — the history is not genuinely concurrent)"
    );
    assert!(merged.is_genuinely_concurrent());

    // The overlapping pairs are genuinely same-key, cross-process, read↔write.
    for &(i, j) in &overlaps {
        let a = &merged.ops()[i];
        let b = &merged.ops()[j];
        assert_ne!(a.process, b.process, "an overlap pair must cross processes");
        assert_eq!(
            a.record.key, b.record.key,
            "an overlap pair must be same-key"
        );
    }

    // The shared register's written version climbs across overwrites (writer-supplied, ADR-0041
    // decision-1 overwrite semantics; the backend-observed strengthening is a later slice).
    let written: Vec<u64> = histories[0]
        .ops()
        .iter()
        .filter(|o| o.kind == OpKind::Put)
        .map(|o| o.version.expect("a PUT records its written version"))
        .collect();
    assert_eq!(
        written.len() as u64,
        ROUNDS,
        "the writer drove every overwrite"
    );
    assert!(
        written.windows(2).all(|w| w[1] > w[0]),
        "the shared register's written version must climb across overwrites"
    );

    // Per-key reads observe a monotone version sequence — no stale/torn read on the linearizable
    // commit-point register (ADR-0041 decision 1).
    assert!(
        merged.reads_monotone_per_key(),
        "the reader must observe a monotone per-key version sequence (no stale/torn read)"
    );
}
