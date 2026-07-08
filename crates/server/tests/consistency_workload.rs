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
    // Every register entry carries its `:key` so the checker can partition per register.
    let expected = concat!(
        "[{:process 0, :type :invoke, :f :write, :key \"k\", :value 1, :time 0}\n",
        " {:process 1, :type :invoke, :f :read, :key \"k\", :value nil, :time 5}\n",
        " {:process 0, :type :ok, :f :write, :key \"k\", :value 1, :time 10}\n",
        " {:process 1, :type :ok, :f :read, :key \"k\", :value 1, :time 15}\n",
        " {:process 0, :type :invoke, :f :write, :key \"k\", :value 2, :time 30}\n",
        " {:process 0, :type :info, :f :write, :key \"k\", :value 2, :time 40}\n",
        " {:process 1, :type :invoke, :f :read, :key \"k\", :value nil, :time 50}\n",
        " {:process 1, :type :ok, :f :read, :key \"k\", :value nil, :time 60}]",
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
            .contains(":type :info, :f :write, :key \"k\", :value 2"),
        "an indeterminate write must carry :info, not a fabricated definite outcome"
    );
}

// ─── (a) Register serializer keeps distinct keys distinct (no single-register collapse) ─

#[test]
fn register_serializer_tags_each_op_with_its_key() {
    // A write of key `a` then a read of key `b`. With the key dropped these collapse into one
    // register, and `PUT a=1 ; GET b=absent` looks like a lost read-your-write on a single
    // register — a false verdict (or, the other way, a real per-key bug is masked). Each op must
    // carry its own `:key`, so distinct keys stay distinct in the serialized history.
    let history = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "a", Some(1), 200, 0, 10),
        proc_op(1, OpKind::Get, "b", None, 404, 5, 15),
    ]);
    let edn = history.to_elle_edn();
    assert!(
        edn.contains(":f :write, :key \"a\", :value 1"),
        "the write of key `a` must be tagged :key \"a\"; got:\n{edn}"
    );
    assert!(
        edn.contains(":f :read, :key \"b\", :value nil"),
        "the read of key `b` must be tagged :key \"b\"; got:\n{edn}"
    );
    assert!(
        edn.contains(":key \"a\"") && edn.contains(":key \"b\""),
        "distinct keys must remain distinct — the history must not collapse into one register"
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

// ─── (d) Directory serializer escapes EDN-significant characters in member names ────────

#[test]
fn directory_serializer_escapes_special_characters_in_member_names() {
    // Object names can contain EDN-significant characters — a quote, a backslash, a newline. The
    // serializer must emit them as a valid escaped EDN string, never raw: a raw quote would close
    // the string early (invalid checker input) and a raw backslash would change the parsed member.
    let member = "a\"b\\c\nd";
    let history = DirectoryHistory::from_records(vec![
        dir_rec(DirOpKind::Create, member, 200, 0, 10),
        dir_rec(DirOpKind::Probe, member, 200, 20, 30),
    ]);
    let edn = history.to_elle_edn();

    // Escaped form present: " → \" , \ → \\ , newline → \n.
    assert!(
        edn.contains("\"a\\\"b\\\\c\\nd\""),
        "the member must be emitted as a properly escaped EDN string; got:\n{edn}"
    );
    // The raw, unescaped member (with a bare quote and a real newline) must never leak through.
    assert!(
        !edn.contains(member),
        "the raw unescaped member must never appear in the serialized EDN"
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
    // definite AtLeast from nothing — so reading v=1 afterward is valid.
    let put_arm = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(2), 500, 0, 10),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 20, 30),
    ]);
    assert!(
        put_arm.session_read_your_writes(),
        "an indeterminate PUT must not create a definite AtLeast obligation from nothing"
    );

    // PUT arm (INV-1 ii-b): an indeterminate write must NOT ERASE a standing committed AtLeast.
    // After a determinate v=5 (AtLeast(5)) an indeterminate v=6 write, reading v=1 is a real
    // read-your-writes violation whether or not the uncertain write took effect — the register
    // only climbs, so the committed floor of 5 still holds. REJECT. (Regression guard for the
    // erase-to-Unknown bug: storing Unknown here would wrongly accept the v=1 read.)
    let put_arm_keeps_standing_atleast = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(5), 200, 0, 10),
        proc_op(0, OpKind::Put, "k", Some(6), 500, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 40, 50),
    ]);
    assert!(
        !put_arm_keeps_standing_atleast.session_read_your_writes(),
        "an indeterminate PUT must not erase a standing committed AtLeast(5); reading v=1 is a violation"
    );

    // PUT arm (INV-1 ii-c): an indeterminate write MAY relax a standing Absent — after a determinate
    // delete (Absent), an indeterminate write may have (re)created the key, so a later present read
    // is not provably a resurrection. ACCEPT. (Guards against over-correcting the fix into a pure
    // no-op that would keep Absent and wrongly reject the v=2 read.)
    let put_arm_relaxes_absent = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Delete, "k", None, 204, 0, 10),
        proc_op(0, OpKind::Put, "k", Some(2), 500, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(2), 200, 40, 50),
    ]);
    assert!(
        put_arm_relaxes_absent.session_read_your_writes(),
        "an indeterminate PUT after a delete may have created the key — a later present read is valid"
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

    // DELETE arm (INV-1 ii-b): an indeterminate delete MAY relax a standing AtLeast to Unknown —
    // after a determinate PUT v=1 (AtLeast(1)) an indeterminate delete may have removed the key, so
    // a later determinate 404 read is not provably an own-write-lost. ACCEPT.
    let delete_arm_relaxes_atleast = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Delete, "k", None, 500, 20, 30),
        proc_op(0, OpKind::Get, "k", None, 404, 40, 50),
    ]);
    assert!(
        delete_arm_relaxes_atleast.session_read_your_writes(),
        "an indeterminate DELETE may have removed the key, so a later 404 read is not own-write-lost"
    );

    // DELETE arm (INV-1 ii-c): an indeterminate delete must NOT erase a standing Absent — a delete
    // can't resurrect a key, so after a determinate delete (Absent) an indeterminate delete leaves
    // the key absent, and a later determinate present read IS a resurrection. REJECT. (Regression
    // guard: storing Unknown here would wrongly accept the v=1 read.)
    let delete_arm_keeps_standing_absent = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Delete, "k", None, 204, 0, 10),
        proc_op(0, OpKind::Delete, "k", None, 500, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 40, 50),
    ]);
    assert!(
        !delete_arm_keeps_standing_absent.session_read_your_writes(),
        "an indeterminate DELETE must not erase a standing Absent; a later present read is a resurrection"
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

// ─── (c) Session RYW is cross-process aware — an own delete/write is not a violation when
//     another process legitimately recreated or removed the key in a merged history (INV-1) ──

#[test]
fn session_read_your_writes_is_cross_process_aware() {
    // ACCEPT — cross-process RECREATE: P0 deletes k, then P1 PUTs k=1 (before P0's read), so P0
    // observing k=1 is a valid read of a LATER write, not a resurrection. RED if the Absent
    // obligation rejects without checking whether another process recreated the key.
    let recreate = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Delete, "k", None, 204, 0, 10),
        proc_op(1, OpKind::Put, "k", Some(1), 200, 15, 25),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 30, 40),
    ]);
    assert!(
        recreate.session_read_your_writes(),
        "a present read after our own delete is valid when another process recreated the key"
    );

    // ACCEPT — cross-process DELETE: P0 PUTs k=1, then P1 deletes k (before P0's read), so P0
    // observing a 404 is a valid read of a LATER delete, not an own-write-lost.
    let removed = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(1, OpKind::Delete, "k", None, 204, 15, 25),
        proc_op(0, OpKind::Get, "k", None, 404, 30, 40),
    ]);
    assert!(
        removed.session_read_your_writes(),
        "a 404 read after our own write is valid when another process deleted the key"
    );

    // REJECT sanity — SINGLE process (no other writer): the resurrection and own-write-lost
    // rejections still fire, so the cross-process relaxation didn't defang the check.
    let single_resurrect = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Delete, "k", None, 204, 0, 10),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 20, 30),
    ]);
    assert!(
        !single_resurrect.session_read_your_writes(),
        "with no other writer, a present read after our own delete is still a resurrection"
    );
    let single_lost = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Get, "k", None, 404, 20, 30),
    ]);
    assert!(
        !single_lost.session_read_your_writes(),
        "with no other writer, a 404 read after our own write is still an own-write-lost"
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

// ─── (c) Read monotonicity ignores OVERLAPPING reads (they have no real-time order) ────

#[test]
fn reads_monotone_ignores_overlapping_reads() {
    // A long read observing v=2 spans [0,100]; a short read observing v=1 spans [10,20], fully
    // inside it. The two OVERLAP, so they have no real-time order — the v=1 read can linearize
    // before the write of v=2. A legal concurrent execution: ACCEPT. RED if the check compares
    // reads in start-time order instead of requiring a strict real-time order.
    let overlapping = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Get, "k", Some(2), 200, 0, 100),
        proc_op(1, OpKind::Get, "k", Some(1), 200, 10, 20),
    ]);
    assert!(
        overlapping.reads_monotone_per_key(),
        "overlapping reads have no real-time order — a lower version inside a longer read is legal"
    );

    // Sanity — the same versions but NON-overlapping (the v=2 read ends before the v=1 read
    // begins) is a genuine real-time-ordered regression and must still be REJECTED.
    let ordered_regression = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Get, "k", Some(2), 200, 0, 10),
        proc_op(1, OpKind::Get, "k", Some(1), 200, 20, 30),
    ]);
    assert!(
        !ordered_regression.reads_monotone_per_key(),
        "a strictly-earlier read that saw a newer version than a later read is a real regression"
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
