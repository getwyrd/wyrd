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
    compose_final_read, consistency_verdict_dispatch, delete_pool_key, membership,
    ConsistencyVerdictDispatch, DirCreate, DirFinalRead, DirectoryHistory, Membership,
    MultiProcessHistory, ProcOp, ELLE_IN_GATE_CMD_VAR, ELLE_OFF_CHECK_VERDICT_JOB,
    REGISTER_OVERWRITE_POOL_KEY,
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

fn dir_create(process: usize, id: u64, status: u16, start: u64, end: u64) -> DirCreate {
    DirCreate {
        process,
        id,
        status,
        start: at(start),
        end: at(end),
    }
}

// ─── (b) Elle-EDN serializer: byte-exact rw-register txn golden, indeterminate → :info ──

#[test]
fn register_serializer_emits_byte_exact_rw_register_txn_edn_with_info_for_indeterminate() {
    // A crafted 2-process register history: P0 overwrites the shared key (v1 determinate, v2
    // INDETERMINATE via a 500), P1 reads it (v1 determinate-present, then a determinate 404
    // absent). The vocabulary is the one elle-cli 0.1.9 ACCEPTS (verified at Plan; #408): every
    // op is `:f :txn` with a micro-op `:value` — `[[:w key v]]` for a write, `[[:r key v]]` for a
    // read — never the #406 scalar `:value` the real checker rejected. The indeterminate PUT must
    // serialize `:info` (never a definite `:ok`), and the determinate 404 read a definite `:ok`
    // reading `nil`.
    let history = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(1, OpKind::Get, "k", Some(1), 200, 5, 15),
        proc_op(0, OpKind::Put, "k", Some(2), 500, 30, 40),
        proc_op(1, OpKind::Get, "k", None, 404, 50, 60),
    ]);

    // The exact bytes (an EDN operation-history vector, one map per line). Written as a literal,
    // NOT recomputed with the serializer's own join, so a delimiter/field/order change is caught.
    // The register key lives INSIDE the txn micro-op (elle partitions per key on it) — no :key.
    let expected = concat!(
        "[{:process 0, :type :invoke, :f :txn, :value [[:w \"k\" 1]], :time 0}\n",
        " {:process 1, :type :invoke, :f :txn, :value [[:r \"k\" nil]], :time 5}\n",
        " {:process 0, :type :ok, :f :txn, :value [[:w \"k\" 1]], :time 10}\n",
        " {:process 1, :type :ok, :f :txn, :value [[:r \"k\" 1]], :time 15}\n",
        " {:process 0, :type :invoke, :f :txn, :value [[:w \"k\" 2]], :time 30}\n",
        " {:process 0, :type :info, :f :txn, :value [[:w \"k\" 2]], :time 40}\n",
        " {:process 1, :type :invoke, :f :txn, :value [[:r \"k\" nil]], :time 50}\n",
        " {:process 1, :type :ok, :f :txn, :value [[:r \"k\" nil]], :time 60}]",
    );

    assert_eq!(
        history.to_elle_edn(),
        expected,
        "the register serializer must emit byte-exact rw-register txn Elle EDN, mapping the \
         indeterminate PUT to :info (never a definite :ok) and the determinate 404 read to a \
         definite :ok reading nil"
    );
    // Guard rail on the load-bearing INV-1 byte: the indeterminate write is `:info`, not :ok.
    assert!(
        history
            .to_elle_edn()
            .contains(":type :info, :f :txn, :value [[:w \"k\" 2]]"),
        "an indeterminate write must carry :info, not a fabricated definite outcome"
    );
    // A register write NEVER emits a nil-write micro-op — `[:w k nil]` was verified at Plan to
    // make even a correct history come back `false`, so it must never appear.
    assert!(
        !history.to_elle_edn().contains(":w \"k\" nil"),
        "a nil-write [:w k nil] is a checker-rejected fabrication and must never be emitted"
    );
}

// ─── (a) Register serializer keeps distinct keys distinct (no single-register collapse) ─

#[test]
fn register_serializer_tags_each_op_with_its_key_inside_the_micro_op() {
    // A write of key `a` then a read of key `b`. The key lives inside the txn micro-op, so distinct
    // keys stay distinct registers (elle partitions per key on the micro-op key) — a read of key
    // `b` after a write of key `a` must not look like same-key traffic.
    let history = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, "a", Some(1), 200, 0, 10),
        proc_op(1, OpKind::Get, "b", None, 404, 5, 15),
    ]);
    let edn = history.to_elle_edn();
    assert!(
        edn.contains("[[:w \"a\" 1]]"),
        "the write of key `a` must carry [[:w \"a\" 1]]; got:\n{edn}"
    );
    assert!(
        edn.contains("[[:r \"b\" nil]]"),
        "the read of key `b` must carry [[:r \"b\" nil]]; got:\n{edn}"
    );
}

// ─── (b) A register DELETE has no rw-register encoding — panics, never a fabricated op ──

#[test]
#[should_panic(expected = "no faithful rw-register")]
fn register_serializer_panics_on_a_delete_never_fabricating_a_representation() {
    // Design §2: register DELETE is excluded by pool construction, never per-op filtering. If a
    // delete reaches the Elle-fed serializer it is a pool-construction bug — the serializer must
    // hard-error rather than silently drop it or fabricate a nil-write.
    let history =
        MultiProcessHistory::from_ops(vec![proc_op(0, OpKind::Delete, "k", None, 204, 0, 10)]);
    let _ = history.to_elle_edn();
}

// ─── (d) Directory-as-set serializer: `set` model — integer :add + composed :read ──────

#[test]
fn directory_serializer_emits_set_model_add_and_composed_read_with_integer_elements() {
    // The `set` vocabulary elle-cli 0.1.9 accepts (verified at Plan): create → `:add` of a unique
    // INTEGER element (a string element crashes the valid case to `:unknown`), plus ONE composed
    // post-heal `:read` of the present set `#{ints}`. The v2 `:remove`/`:contains` vocabulary is
    // gone (verified rejected). An INDETERMINATE create (503) is `:info`, and its uncertain
    // element is NOT fabricated into the composed read.
    let history = DirectoryHistory::from_set_run(
        vec![
            dir_create(0, 1, 200, 0, 10),
            dir_create(1, 2, 200, 5, 15),
            dir_create(0, 3, 503, 20, 30),
        ],
        Some(DirFinalRead {
            process: 0,
            present: vec![1, 2],
            unresolved: Vec::new(),
            start: at(40),
            end: at(50),
        }),
    );

    let expected = concat!(
        "[{:process 0, :type :invoke, :f :add, :value 1, :time 0}\n",
        " {:process 1, :type :invoke, :f :add, :value 2, :time 5}\n",
        " {:process 0, :type :ok, :f :add, :value 1, :time 10}\n",
        " {:process 1, :type :ok, :f :add, :value 2, :time 15}\n",
        " {:process 0, :type :invoke, :f :add, :value 3, :time 20}\n",
        " {:process 0, :type :info, :f :add, :value 3, :time 30}\n",
        " {:process 0, :type :invoke, :f :read, :value nil, :time 40}\n",
        " {:process 0, :type :ok, :f :read, :value #{1 2}, :time 50}]",
    );

    assert_eq!(
        history.to_elle_edn(),
        expected,
        "the directory serializer must emit the `set` model: integer :add ops + one composed \
         :read of #{{present ints}}, indeterminate create → :info"
    );
    let edn = history.to_elle_edn();
    // The checker-rejected v2 vocabulary must never appear.
    assert!(
        !edn.contains(":remove") && !edn.contains(":contains"),
        "the set model must contain only :add/:read, never :remove/:contains: {edn}"
    );
    // Elements are integers, never quoted strings.
    assert!(
        !edn.contains(":add, :value \""),
        "set elements must be integers, never strings (a string crashes the checker): {edn}"
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

// ─── (d2) The composed post-heal final read: an unknown member is never a silent absence ──

/// The composed final read is a claim about the WHOLE set, so an unresolved member may not simply
/// be dropped from it: in the `set` model an acknowledged `:add` missing from a definite `:ok`
/// read is a **lost element**, and the real elle-cli returns `false` for exactly that shape (the
/// committed `directory-history-known-bad.edn` fixture). Dropping an unknown probe would therefore
/// fabricate a violation out of a question the sweep never got an answer to — INV-1 in the
/// absence direction. It degrades the composed read to `:info` instead.
#[test]
fn an_unresolved_probe_degrades_the_composed_read_to_info_rather_than_omitting_the_member() {
    // Member 1 probed present (200), member 2's probe FAILED (503 — what a nemesis induces), and
    // member 3 probed genuinely absent (404).
    let read = compose_final_read(0, &[(1, 200), (2, 503), (3, 404)], at(40), at(50));

    assert_eq!(
        read.present,
        vec![1],
        "only a determinate 200 proves presence"
    );
    assert_eq!(
        read.unresolved,
        vec![2],
        "an indeterminate probe must be RECORDED as unresolved, never silently dropped — \
         dropping it states a fabricated absence, which the `set` model reads as a lost element"
    );
    assert!(
        !read.is_determinate(),
        "a sweep with an unresolved member composed no definite set"
    );

    // …and the serializer must state that uncertainty, not paper over it.
    let edn = DirectoryHistory::from_set_run(
        vec![dir_create(0, 1, 200, 0, 10), dir_create(0, 2, 200, 5, 15)],
        Some(read),
    )
    .to_elle_edn();
    assert!(
        edn.contains("{:process 0, :type :info, :f :read, :value nil, :time 50}"),
        "an indeterminate composed read must complete :info with a nil value (verified against \
         elle-cli 0.1.9: the model then returns `:unknown` ⇒ INCONCLUSIVE, never a vacuous \
         pass): {edn}"
    );
    assert!(
        !edn.contains(":type :ok, :f :read"),
        "an indeterminate sweep must NEVER emit a definite :ok read — that claims the unresolved \
         members are absent, i.e. a fabricated lost element: {edn}"
    );
}

/// The dual, so the degrade cannot be "fixed" by making every read indeterminate: a sweep that
/// resolved every member still emits a definite `:ok` read — including the genuinely **absent**
/// (404) member, which is a real observation and must stay visible to the checker as a lost
/// element. Suppressing that is how a real violation would get hidden.
#[test]
fn a_fully_resolved_sweep_composes_a_definite_read_that_still_exposes_a_lost_element() {
    // Member 2 was created but probes 404: definitely gone.
    let read = compose_final_read(0, &[(1, 200), (2, 404)], at(40), at(50));
    assert!(read.is_determinate());
    assert_eq!(read.present, vec![1]);
    assert!(
        read.unresolved.is_empty(),
        "a determinate 404 is an OBSERVATION of absence, not an unresolved probe"
    );

    let edn = DirectoryHistory::from_set_run(
        vec![dir_create(0, 1, 200, 0, 10), dir_create(0, 2, 200, 5, 15)],
        Some(read),
    )
    .to_elle_edn();
    assert!(
        edn.contains("{:process 0, :type :ok, :f :read, :value #{1}, :time 50}"),
        "a fully resolved sweep composes a definite :ok read whose set omits the definitely-absent \
         member — the shape the real checker judges `false` as a lost element: {edn}"
    );
}

/// The report's "history size" must count the **checked** history, not the sweep's raw probes: the
/// whole universe sweep enters the EDN as ONE composed `:read`. Counting probes as ops overstates
/// the checked directory history ~2x in the one field an outsider judges the run's weight by.
#[test]
fn directory_op_count_counts_the_composed_read_once_not_every_probe() {
    let creates = vec![dir_create(0, 1, 200, 0, 10), dir_create(1, 2, 200, 5, 15)];
    let swept = compose_final_read(0, &[(1, 200), (2, 200)], at(40), at(50));

    let with_read = DirectoryHistory::from_set_run(creates.clone(), Some(swept));
    assert_eq!(
        with_read.op_count(),
        3,
        "2 creates + ONE composed read — never 2 creates + one probe per swept member"
    );
    assert_eq!(
        DirectoryHistory::from_set_run(creates, None).op_count(),
        2,
        "an add-only history has no composed read to count"
    );
}

// ─── (c0) Delete-pool construction: single writer per key (the INV-1 dual) ─────────────

/// **The delete pool's keys must be single-writer-by-construction.** The version tag this workload
/// writes is client-assigned, so it orders by *writer*, not by *commit* — and all three #406 checks
/// judging this pool compare raw version tags on a key. With two writers on ONE key (e.g. disjoint
/// version *bands*), a perfectly linearizable execution makes those checks report `false`, and the
/// runner escalates that to "a real violation observed on the live cluster": a fabricated violation
/// that would wreck the credibility artifact.
///
/// This pins the fix at its premise. `delete_pool_key` gives each process its own key, so this
/// linearizable history — built with the SAME production key assignment the live scenario uses —
/// must be judged clean by all three checks.
#[test]
fn delete_pool_keys_are_single_writer_per_key_so_version_tags_track_commit_order() {
    // The keys are disjoint per process, and disjoint from the Elle-fed overwrite pool's key
    // (which is what makes excluding the delete traffic from the register EDN sound at all).
    assert_ne!(
        delete_pool_key(0),
        delete_pool_key(1),
        "each delete-pool process must own its key: a shared key has multiple writers, and a \
         client-assigned version tag then no longer tracks commit order"
    );
    assert_ne!(delete_pool_key(0), REGISTER_OVERWRITE_POOL_KEY);
    assert_ne!(delete_pool_key(1), REGISTER_OVERWRITE_POOL_KEY);

    // A linearizable interleaving of the pool's real traffic (PUT → read-your-write → DELETE →
    // read-after-delete), two processes, on the production keys. p1's writes commit BETWEEN p0's —
    // the interleaving that a shared key would misjudge.
    let (k0, k1) = (delete_pool_key(0), delete_pool_key(1));
    let history = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, &k0, Some(1), 200, 0, 10),
        proc_op(1, OpKind::Put, &k1, Some(1), 200, 12, 20),
        proc_op(1, OpKind::Get, &k1, Some(1), 200, 22, 30),
        proc_op(0, OpKind::Put, &k0, Some(2), 200, 32, 40),
        proc_op(0, OpKind::Get, &k0, Some(2), 200, 42, 50),
        proc_op(1, OpKind::Delete, &k1, None, 204, 52, 60),
        proc_op(1, OpKind::Get, &k1, None, 404, 62, 70),
        proc_op(0, OpKind::Get, &k0, Some(2), 200, 72, 80),
    ]);

    assert!(
        history.session_read_your_writes(),
        "a linearizable single-writer-per-key delete pool must not violate read-your-writes"
    );
    assert!(
        history.session_monotonic_reads(),
        "a linearizable single-writer-per-key delete pool must not violate monotonic reads"
    );
    assert!(
        history.reads_monotone_per_key(),
        "a linearizable single-writer-per-key delete pool must not violate per-key read \
         monotonicity"
    );
}

/// The refutation that makes the test above load-bearing rather than decorative: the SAME
/// linearizable execution, re-keyed onto ONE shared key with disjoint version bands (`p0: 1..`,
/// `p1: 1_000_000..`), is reported as a violation by all three production checks. p0's `v=2`
/// commits *after* p1's `v=1_000_001`, so a later read legitimately observes the smaller tag.
///
/// The checks are not wrong — they are #406's landed, INV-1-sound checks and stay untouched (the
/// brief's scope). Their premise (tag order = commit order) is what a shared key breaks. This test
/// documents the trap so nobody "simplifies" the pool back onto one key: if a future change makes
/// `delete_pool_key` return a shared key, the test above goes red and this one explains why.
#[test]
fn a_shared_delete_pool_key_with_version_bands_would_fabricate_a_violation() {
    const SHARED: &str = "checked-delete-register";
    let banded = MultiProcessHistory::from_ops(vec![
        proc_op(0, OpKind::Put, SHARED, Some(1), 200, 0, 10),
        proc_op(1, OpKind::Put, SHARED, Some(1_000_001), 200, 12, 20),
        proc_op(1, OpKind::Get, SHARED, Some(1_000_001), 200, 22, 30),
        // p0's v=2 commits AFTER p1's v=1_000_001 — a newer commit carrying a SMALLER tag.
        proc_op(0, OpKind::Put, SHARED, Some(2), 200, 32, 40),
        proc_op(0, OpKind::Get, SHARED, Some(2), 200, 42, 50),
        proc_op(1, OpKind::Get, SHARED, Some(2), 200, 52, 60),
    ]);

    assert!(
        !banded.reads_monotone_per_key(),
        "the shared-key banded shape must be exhibited as the trap it is: p1's read of the \
         smaller-but-newer tag reads as a regression, so a CORRECT system is reported violating"
    );
    assert!(
        !banded.session_read_your_writes(),
        "p1 reads v=2 after its own v=1_000_001 write — a fabricated read-your-writes violation"
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

    // REJECT — the other process's write is ordered BEFORE our delete, so it cannot recreate the
    // key afterward: P1 PUT k [0,10] (ends before) → P0 DELETE k [20,30] → P0 GET k=v1 [40,50] is
    // still a resurrection. RED if the waiver accepts any earlier cross-process PUT regardless of
    // whether it could linearize after our delete.
    let pre_delete_put = MultiProcessHistory::from_ops(vec![
        proc_op(1, OpKind::Put, "k", Some(1), 200, 0, 10),
        proc_op(0, OpKind::Delete, "k", None, 204, 20, 30),
        proc_op(0, OpKind::Get, "k", Some(1), 200, 40, 50),
    ]);
    assert!(
        !pre_delete_put.session_read_your_writes(),
        "a cross-process PUT ordered before our delete cannot recreate the key — still a resurrection"
    );

    // REJECT — the other process's delete is ordered BEFORE our write, so it cannot erase it: P1
    // DELETE k [0,10] → P0 PUT k=1 [20,30] → P0 GET 404 [40,50] is still an own-write-lost. RED if
    // the waiver accepts any earlier cross-process DELETE.
    let pre_write_delete = MultiProcessHistory::from_ops(vec![
        proc_op(1, OpKind::Delete, "k", None, 204, 0, 10),
        proc_op(0, OpKind::Put, "k", Some(1), 200, 20, 30),
        proc_op(0, OpKind::Get, "k", None, 404, 40, 50),
    ]);
    assert!(
        !pre_write_delete.session_read_your_writes(),
        "a cross-process DELETE ordered before our write cannot erase it — still an own-write-lost"
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
