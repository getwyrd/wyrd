//! The **concurrent consistency workload substrate** for the #329 checker (ADR-0041
//! §Decision, #329 slice 3). It builds, on top of the landed #405 networked observable
//! ([`crate::consistency_observable`]), everything a recognized register/list-append checker
//! (**Elle**) needs to consume — while keeping the *linearizability verdict itself* off-Check
//! (ADR-0041/ADR-0016: no JVM/Clojure in `cargo xtask ci`):
//!
//! 1. a **multi-process history** ([`MultiProcessHistory`]) that merges the per-client
//!    [`History`]s into one real-time-ordered log, each op tagged with its client's
//!    `:process` id;
//! 2. a **concurrency witness** ([`MultiProcessHistory::is_genuinely_concurrent`]) that counts
//!    an overlap ONLY when it constrains a single register (INV-2);
//! 3. an **Elle-EDN serializer** ([`MultiProcessHistory::to_elle_edn`] /
//!    [`DirectoryHistory::to_elle_edn`]) that maps every **indeterminate** wire outcome to
//!    `:info`, never a definite `:ok`/`:fail` (INV-1);
//! 4. **session** read-your-writes + monotonic-read checks and a **per-key** read-monotonicity
//!    check, all *sound* — an indeterminate op never establishes a definite obligation and is
//!    never counted as a violation (INV-1);
//! 5. a **directory-as-set** history ([`DirectoryHistory`]) — create=PUT / delete=DELETE /
//!    membership=GET-probe (no rename, no wire `LIST`) — serialized to the checker's set op
//!    form, with indeterminate probes mapped to `:info` (no fabricated `[member false]`);
//! 6. a **verdict-dispatch** value ([`consistency_verdict_dispatch`]) that routes the Elle
//!    verdict to the privileged off-Check job — representable + unit-tested, never a JVM
//!    shell-out into `ci` (mirrors `xtask/src/metadata_faults.rs`).
//!
//! # The two soundness invariants this substrate must hold SURFACE-WIDE
//!
//! **INV-1 (no fabricated certainty).** No function here may turn an **indeterminate** wire
//! outcome (5xx / timeout / synthetic-0 status — see [`is_indeterminate`]) into a **definite**
//! obligation, completion type, or membership claim. An indeterminate op is `:info` ("may or
//! may not have happened"); a local check **SKIPS** it. Enforced across every arm: the register
//! completion-type (see [`register_completion_keyword`]); the directory completion-type and its
//! membership derivation (see [`membership`]); the RYW PUT/DELETE obligation arms and the RYW
//! read side (see [`MultiProcessHistory::session_read_your_writes`]); and the monotonic-read
//! checks (see [`MultiProcessHistory::session_monotonic_reads`] and
//! [`MultiProcessHistory::reads_monotone_per_key`]).
//!
//! **INV-2 (non-vacuity).** An overlap counts as genuine concurrency ONLY when it imposes an
//! ordering constraint on **one** register — **same-key**, **read↔write**, across **distinct
//! processes** (see [`MultiProcessHistory::read_write_overlapping_pairs_across_processes`]).
//! Read↔read and cross-key overlaps are vacuous and must NOT count.
//!
//! The linearizability *verdict* stays Elle's, off-Check (ADR-0041 §Decision): this module
//! produces, records, and serializes the history and asserts only *sound, local* invariants —
//! it does **not** re-derive a global register/namespace-linearizability decision in-gate.

use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::consistency_observable::{History, OpKind, OpRecord};

/// A wire outcome whose **effect is unknown**: a 5xx server error, a synthetic-0 status the
/// client stamps on a timeout, or any status ≥ 500. In Jepsen/Elle semantics such an op is
/// `:info` — it "may or may not have happened" — so no local check may derive a definite
/// obligation or outcome from it (INV-1). A determinate 4xx (e.g. a 404 absent read, a 403
/// refusal) is **not** indeterminate: its effect is known.
#[must_use]
pub fn is_indeterminate(status: u16) -> bool {
    status == 0 || status >= 500
}

// ─── Multi-process history ────────────────────────────────────────────────────────────

/// One operation tagged with the id of the client **process** (`:process`) that observed it —
/// the unit a multi-process register history is built from. `record` is the client-observed
/// [`OpRecord`] from the landed #405 observable; `process` is the client's id.
#[derive(Debug, Clone)]
pub struct ProcOp {
    /// The client process id that observed this op (its `:process` in Elle's history).
    pub process: usize,
    /// The client-observed operation record (kind, key, version, status, real-time span).
    pub record: OpRecord,
}

/// A merged, real-time-ordered, **`:process`-tagged** history over several concurrent clients
/// — the multi-process register history a linearizability checker consumes. Built by
/// [`merge`](MultiProcessHistory::merge)ing per-client histories (or, for crafted tests, from
/// explicit [`ProcOp`]s via [`from_ops`](MultiProcessHistory::from_ops)).
#[derive(Debug, Default, Clone)]
pub struct MultiProcessHistory {
    ops: Vec<ProcOp>,
}

impl MultiProcessHistory {
    /// Merge per-client histories into one real-time-ordered log, tagging every op with its
    /// client's `:process` id (its index in `histories`) and sorting the merged log by the
    /// client-observed real-time span (`start`, then `end`). This is the multi-process
    /// history #329's checker needs: a single log across ≥2 concurrent clients in which
    /// same-key read↔write spans from distinct processes can overlap.
    #[must_use]
    pub fn merge(histories: &[History]) -> Self {
        let mut ops: Vec<ProcOp> = Vec::new();
        for (process, history) in histories.iter().enumerate() {
            for record in history.ops() {
                ops.push(ProcOp {
                    process,
                    record: record.clone(),
                });
            }
        }
        ops.sort_by(|a, b| {
            a.record
                .start
                .cmp(&b.record.start)
                .then(a.record.end.cmp(&b.record.end))
        });
        Self { ops }
    }

    /// Build a history from explicit process-tagged ops, preserving the given order — the
    /// crafted-history constructor the socket-free tests feed determinate/indeterminate
    /// witnesses through.
    #[must_use]
    pub fn from_ops(ops: Vec<ProcOp>) -> Self {
        Self { ops }
    }

    /// The merged ops, in real-time order.
    #[must_use]
    pub fn ops(&self) -> &[ProcOp] {
        &self.ops
    }

    /// Non-vacuous **and** every op individually well-formed (a real `start <= end` span): the
    /// bar a checker input must clear. An empty or malformed multi-process history proves
    /// nothing.
    #[must_use]
    pub fn well_formed(&self) -> bool {
        !self.ops.is_empty() && self.ops.iter().all(|op| op.record.well_formed())
    }

    /// The overlapping op pairs (as index pairs into [`ops`](Self::ops)) that impose an
    /// ordering constraint on a **single register** — the ONLY overlaps that are non-vacuous
    /// (INV-2). A pair `(i, j)` counts iff **all** hold:
    ///
    /// * **distinct processes** (`a.process != b.process`) — an intra-process pair is
    ///   sequential, not concurrent;
    /// * **same key** (`a.record.key == b.record.key`) — a cross-key overlap constrains no
    ///   single register (vacuous);
    /// * **read↔write** — exactly one is a read (GET) and the other a write (PUT/DELETE); a
    ///   read↔read overlap constrains no register (vacuous);
    /// * **real-time-overlapping spans** (`a.start <= b.end && b.start <= a.end`).
    #[must_use]
    pub fn read_write_overlapping_pairs_across_processes(&self) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        for i in 0..self.ops.len() {
            for j in (i + 1)..self.ops.len() {
                let a = &self.ops[i];
                let b = &self.ops[j];
                if a.process != b.process
                    && a.record.key == b.record.key
                    && is_read_write_pair(a.record.kind, b.record.kind)
                    && spans_overlap(&a.record, &b.record)
                {
                    pairs.push((i, j));
                }
            }
        }
        pairs
    }

    /// Genuinely concurrent iff at least one same-key read↔write overlap across distinct
    /// processes exists (INV-2). A history of only read↔read or only cross-key overlaps is
    /// **not** genuinely concurrent — those constrain no single register.
    #[must_use]
    pub fn is_genuinely_concurrent(&self) -> bool {
        !self
            .read_write_overlapping_pairs_across_processes()
            .is_empty()
    }

    /// **Session read-your-writes** (ADR-0015 guarantee 3), sound (INV-1). Per process, per
    /// key, a **determinate** PUT of `v` establishes an `AtLeast(v)` obligation and a
    /// **determinate** DELETE establishes an `Absent` obligation; a later determinate read on
    /// that key must satisfy the standing obligation. Returns `false` iff some read
    /// *definitely* violates one.
    ///
    /// INV-1 across **every** arm, not only the read arm:
    /// * the **PUT arm** guards [`is_indeterminate`]: an indeterminate PUT does NOT establish
    ///   `AtLeast(v)` (it clears the obligation to unknown) — so
    ///   `[PUT k v=2 status=500; GET k v=1 status=200]` is **accepted**;
    /// * the **DELETE arm** guards [`is_indeterminate`]: an indeterminate DELETE does NOT
    ///   establish `Absent` — so `[PUT k v=1 200; DELETE k 500; GET k v=1 200]` is **accepted**;
    /// * the **read arm** treats an indeterminate GET as **no observation** (skipped), keyed on
    ///   the status, not on `version == None` — a crafted indeterminate GET carrying a
    ///   `Some(_)` version is still not counted as a resurrection/regression.
    #[must_use]
    pub fn session_read_your_writes(&self) -> bool {
        for ops in self.by_process().values() {
            let mut obligation: HashMap<&str, Obligation> = HashMap::new();
            for op in ops {
                let key = op.record.key.as_str();
                let status = op.record.status;
                match op.record.kind {
                    OpKind::Put => {
                        if is_indeterminate(status) {
                            // INV-1: an indeterminate write establishes no definite lower bound.
                            obligation.insert(key, Obligation::Unknown);
                        } else if is_success(status) {
                            obligation
                                .insert(key, Obligation::AtLeast(op.record.version.unwrap_or(0)));
                        }
                        // A determinate-failed PUT had no effect: the obligation is unchanged.
                    }
                    OpKind::Delete => {
                        if is_indeterminate(status) {
                            // INV-1: an indeterminate delete establishes no definite absence.
                            obligation.insert(key, Obligation::Unknown);
                        } else if is_success(status) || status == 404 {
                            obligation.insert(key, Obligation::Absent);
                        }
                        // A determinate-failed DELETE had no effect: obligation unchanged.
                    }
                    OpKind::Get => {
                        if is_indeterminate(status) {
                            // INV-1: an indeterminate read is no observation — never a violation.
                            continue;
                        }
                        let obl = obligation.get(key).copied().unwrap_or(Obligation::Unknown);
                        match op.record.version {
                            // A determinate present read of version `v`.
                            Some(v) => match obl {
                                Obligation::Absent => return false, // resurrected after own delete
                                Obligation::AtLeast(w) if v < w => return false, // read older than own write
                                _ => {}
                            },
                            // `None` version means only "status != 200", which is a definite
                            // absence ONLY for a determinate 404. Any other determinate non-200
                            // read (403/409/412/416/…) observed nothing about the register, so it
                            // must NOT be counted an own-write-lost (INV-1: no fabricated absence).
                            None => {
                                if status == 404 && matches!(obl, Obligation::AtLeast(_)) {
                                    return false; // own write lost: read absent (404) after writing
                                }
                            }
                        }
                    }
                }
            }
        }
        true
    }

    /// **Session monotonic reads** (ADR-0015 guarantee 3), sound (INV-1). Per process, per
    /// key, successive **determinate** reads never observe an older register version. An
    /// indeterminate GET is skipped (keyed on the status via [`is_indeterminate`], **not** on
    /// `version == None`, so a crafted indeterminate read carrying a stale `Some(_)` version is
    /// not counted as a regression); an absent (404) read establishes no version to compare.
    #[must_use]
    pub fn session_monotonic_reads(&self) -> bool {
        for ops in self.by_process().values() {
            if !reads_are_monotone(ops.iter().copied()) {
                return false;
            }
        }
        true
    }

    /// **Per-key read monotonicity** across the whole (multi-process) history, sound (INV-1):
    /// on each key, the determinate reads observed over real time never regress — the reused
    /// register-monotonicity invariant, guarded against indeterminate reads exactly as the
    /// session checks are. Unlike [`session_monotonic_reads`](Self::session_monotonic_reads)
    /// this spans processes, so a global stale read (one process reads a version another
    /// already observed as superseded) is caught.
    #[must_use]
    pub fn reads_monotone_per_key(&self) -> bool {
        reads_are_monotone(self.ops.iter())
    }

    /// Serialize to **Elle's EDN operation-history format** — one `:invoke` entry at each op's
    /// `start` and one completion (`:ok` / `:fail` / `:info`) at its `end`, the whole flat log
    /// sorted by relative time. Every field the criterion names is present:
    /// `:process` / `:type` / `:f` / `:value` / `:time`. INV-1: an **indeterminate** completion
    /// is `:info` (never a definite `:ok`/`:fail`), see [`register_completion_keyword`].
    ///
    /// `:time` is nanoseconds relative to the earliest `start` in the history (Jepsen's
    /// test-relative clock), so the bytes are stable and small.
    ///
    /// This in-gate serialization proves the serializer is **stable and well-shaped**; it does
    /// NOT by itself prove real-Elle-parser acceptance — that is the deferred, off-Check
    /// verdict leg (ADR-0041), over the SAME serialized history.
    #[must_use]
    pub fn to_elle_edn(&self) -> String {
        if self.ops.is_empty() {
            return "[]".to_string();
        }
        let base = self
            .ops
            .iter()
            .map(|op| op.record.start)
            .min()
            .unwrap_or(UNIX_EPOCH);
        let mut entries: Vec<Entry> = Vec::with_capacity(self.ops.len() * 2);
        for op in &self.ops {
            let f = register_f(op.record.kind);
            entries.push(Entry {
                time: rel_nanos(base, op.record.start),
                process: op.process,
                phase: Phase::Invoke,
                rendered: render_entry(
                    op.process,
                    "invoke",
                    f,
                    &register_invoke_value(op.record.kind, op.record.version),
                    rel_nanos(base, op.record.start),
                ),
            });
            entries.push(Entry {
                time: rel_nanos(base, op.record.end),
                process: op.process,
                phase: Phase::Complete,
                rendered: render_entry(
                    op.process,
                    register_completion_type(op.record.kind, op.record.status).keyword(),
                    f,
                    &register_completion_value(op.record.kind, op.record.version),
                    rel_nanos(base, op.record.end),
                ),
            });
        }
        render_history(entries)
    }

    /// Group the ops by process, preserving each process's relative (real-time) order — the
    /// per-session view the session checks walk.
    fn by_process(&self) -> BTreeMap<usize, Vec<&ProcOp>> {
        let mut by: BTreeMap<usize, Vec<&ProcOp>> = BTreeMap::new();
        for op in &self.ops {
            by.entry(op.process).or_default().push(op);
        }
        by
    }
}

/// The standing per-key obligation a session's determinate ops establish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Obligation {
    /// No determinate obligation stands (including after an *indeterminate* mutation, whose
    /// effect is unknown — INV-1).
    Unknown,
    /// A determinate PUT of this version stands: a later determinate read must observe ≥ it.
    AtLeast(u64),
    /// A determinate DELETE stands: a later determinate read must observe absence.
    Absent,
}

/// A read (GET) versus a write (PUT/DELETE): exactly one of each. A read↔read or write↔write
/// pair is not a read↔write pair.
fn is_read_write_pair(a: OpKind, b: OpKind) -> bool {
    (is_read(a) && is_write(b)) || (is_write(a) && is_read(b))
}

fn is_read(kind: OpKind) -> bool {
    matches!(kind, OpKind::Get)
}

fn is_write(kind: OpKind) -> bool {
    matches!(kind, OpKind::Put | OpKind::Delete)
}

/// Real-time spans overlap iff neither strictly precedes the other.
fn spans_overlap(a: &OpRecord, b: &OpRecord) -> bool {
    a.start <= b.end && b.start <= a.end
}

fn is_success(status: u16) -> bool {
    (200..300).contains(&status)
}

/// Determinate-read monotonicity per key over an op sequence in real-time order (INV-1): an
/// indeterminate GET is skipped on its **status**, an absent (404) read establishes no version,
/// and a determinate present read that observes an older version than one already seen on the
/// same key is a regression.
fn reads_are_monotone<'a>(ops: impl Iterator<Item = &'a ProcOp>) -> bool {
    let mut last: HashMap<&str, u64> = HashMap::new();
    for op in ops {
        if op.record.kind != OpKind::Get {
            continue;
        }
        if is_indeterminate(op.record.status) {
            // INV-1: an indeterminate read is not a monotonicity violation.
            continue;
        }
        let Some(v) = op.record.version else {
            // A determinate absent (404) read establishes no version to compare.
            continue;
        };
        match last.get(op.record.key.as_str()) {
            Some(&prev) if v < prev => return false,
            _ => {
                last.insert(op.record.key.as_str(), v);
            }
        }
    }
    true
}

// ─── Elle-EDN rendering (shared by the register and directory serializers) ────────────

/// A completion `:type` in Elle's operation-history vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionType {
    /// The op definitely took effect.
    Ok,
    /// The op definitely did **not** take effect.
    Fail,
    /// The op's effect is **unknown** (indeterminate) — `:info` (INV-1).
    Info,
}

impl CompletionType {
    fn keyword(self) -> &'static str {
        match self {
            CompletionType::Ok => "ok",
            CompletionType::Fail => "fail",
            CompletionType::Info => "info",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Invoke,
    Complete,
}

struct Entry {
    time: u128,
    process: usize,
    phase: Phase,
    rendered: String,
}

/// Sort the flat entry log by `(time, process, phase)` — deterministic, and at equal times an
/// `:invoke` precedes its completion — then wrap it as an EDN vector, one map per line.
fn render_history(mut entries: Vec<Entry>) -> String {
    entries.sort_by(|a, b| {
        a.time
            .cmp(&b.time)
            .then(a.process.cmp(&b.process))
            .then((a.phase as u8).cmp(&(b.phase as u8)))
    });
    let rendered: Vec<String> = entries.into_iter().map(|e| e.rendered).collect();
    format!("[{}]", rendered.join("\n "))
}

/// Render one operation-history map: `{:process P, :type :T, :f :F, :value V, :time N}`.
fn render_entry(process: usize, type_kw: &str, f: &str, value: &str, time: u128) -> String {
    format!("{{:process {process}, :type :{type_kw}, :f :{f}, :value {value}, :time {time}}}")
}

/// Nanoseconds of `t` relative to `base` (the history's earliest start).
fn rel_nanos(base: SystemTime, t: SystemTime) -> u128 {
    t.duration_since(base).unwrap_or_default().as_nanos()
}

/// An optional register version as an EDN value: the decimal digits, or `nil` for absent.
fn edn_version(version: Option<u64>) -> String {
    match version {
        Some(v) => v.to_string(),
        None => "nil".to_string(),
    }
}

fn register_f(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Put => "write",
        OpKind::Get => "read",
        OpKind::Delete => "delete",
    }
}

/// The `:value` on the **invoke** entry: a write carries its intended version, a read/delete
/// carries `nil` (the read result is unknown at invoke time).
fn register_invoke_value(kind: OpKind, version: Option<u64>) -> String {
    match kind {
        OpKind::Put => edn_version(version),
        OpKind::Get | OpKind::Delete => "nil".to_string(),
    }
}

/// The `:value` on the **completion** entry: a write echoes its version, a read carries the
/// version observed (`nil` if absent), a delete carries `nil`.
fn register_completion_value(kind: OpKind, version: Option<u64>) -> String {
    match kind {
        OpKind::Put | OpKind::Get => edn_version(version),
        OpKind::Delete => "nil".to_string(),
    }
}

/// The register completion `:type` **keyword** (without the leading colon) a `(kind, status)`
/// maps to — `"ok"`, `"fail"`, or `"info"`. Public so the INV-1 completion-type arm is
/// directly assertable: an **indeterminate** status MUST map to `"info"` (never a definite
/// `"ok"`/`"fail"`), a determinate success maps to `"ok"` (a GET's 404 absent read is a
/// *successful* read of `nil`), and any other determinate status maps to `"fail"`.
#[must_use]
pub fn register_completion_keyword(kind: OpKind, status: u16) -> &'static str {
    register_completion_type(kind, status).keyword()
}

fn register_completion_type(kind: OpKind, status: u16) -> CompletionType {
    if is_indeterminate(status) {
        return CompletionType::Info;
    }
    let ok = match kind {
        OpKind::Put => is_success(status),
        OpKind::Get | OpKind::Delete => is_success(status) || status == 404,
    };
    if ok {
        CompletionType::Ok
    } else {
        CompletionType::Fail
    }
}

/// The directory completion `:type` **keyword** a `(op, status)` maps to — the directory
/// analogue of [`register_completion_keyword`], so the INV-1 directory completion-type arm is
/// directly assertable: an indeterminate probe MUST map to `"info"`.
#[must_use]
pub fn dir_completion_keyword(op: DirOpKind, status: u16) -> &'static str {
    dir_completion_type(op, status).keyword()
}

// ─── Directory-as-set history ─────────────────────────────────────────────────────────

/// A directory-as-set operation: create appends a member (a PUT under the prefix), delete
/// removes it (a DELETE), and a membership probe (a GET-probe) asks whether one member is
/// present. There is **no** rename and **no** wire `LIST` — the S3 wire floor is PUT/GET/DELETE
/// only (`crates/gateway-s3/src/lib.rs:347`), so the set is modelled by single-member probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirOpKind {
    /// Add a member to the set (create = PUT).
    Create,
    /// Remove a member from the set (delete = DELETE).
    Delete,
    /// Probe one member's presence (membership = GET-probe).
    Probe,
}

/// One directory-as-set operation record, tagged with the client `:process` and its real-time
/// span — the directory analogue of [`OpRecord`].
#[derive(Debug, Clone)]
pub struct DirRecord {
    /// The client process id that observed this op.
    pub process: usize,
    /// Which set operation this records.
    pub op: DirOpKind,
    /// The member (object name under the prefix) the op targeted.
    pub member: String,
    /// The HTTP status the wire returned (200/204/404/…).
    pub status: u16,
    /// Real time just before the request began.
    pub start: SystemTime,
    /// Real time just after the response was fully read.
    pub end: SystemTime,
}

/// A recorded directory-as-set history — the namespace-membership analogue of
/// [`MultiProcessHistory`], serialized to the checker's set op form.
#[derive(Debug, Default, Clone)]
pub struct DirectoryHistory {
    ops: Vec<DirRecord>,
}

impl DirectoryHistory {
    /// Build a directory history from explicit records, preserving order.
    #[must_use]
    pub fn from_records(ops: Vec<DirRecord>) -> Self {
        Self { ops }
    }

    /// The recorded set operations, in order.
    #[must_use]
    pub fn ops(&self) -> &[DirRecord] {
        &self.ops
    }

    /// Serialize to the checker's **set op form** in Elle's EDN: create → `:add`, delete →
    /// `:remove`, probe → `:contains` carrying `[member present?]`. INV-1: an **indeterminate**
    /// probe is `:info` with `[member nil]` — a fabricated `[member false]` is never emitted
    /// (see [`membership`]).
    #[must_use]
    pub fn to_elle_edn(&self) -> String {
        if self.ops.is_empty() {
            return "[]".to_string();
        }
        let base = self
            .ops
            .iter()
            .map(|op| op.start)
            .min()
            .unwrap_or(UNIX_EPOCH);
        let mut entries: Vec<Entry> = Vec::with_capacity(self.ops.len() * 2);
        for op in &self.ops {
            let f = dir_f(op.op);
            entries.push(Entry {
                time: rel_nanos(base, op.start),
                process: op.process,
                phase: Phase::Invoke,
                rendered: render_entry(
                    op.process,
                    "invoke",
                    f,
                    &dir_invoke_value(op.op, &op.member),
                    rel_nanos(base, op.start),
                ),
            });
            entries.push(Entry {
                time: rel_nanos(base, op.end),
                process: op.process,
                phase: Phase::Complete,
                rendered: render_entry(
                    op.process,
                    dir_completion_type(op.op, op.status).keyword(),
                    f,
                    &dir_completion_value(op.op, &op.member, op.status),
                    rel_nanos(base, op.end),
                ),
            });
        }
        render_history(entries)
    }
}

/// A directory member's derived presence from a membership-probe status (INV-1):
/// `200 → Present`, `404 → Absent`, **everything else → Unknown** — never definitely-absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Membership {
    /// The member is present (a determinate 200 probe).
    Present,
    /// The member is absent (a determinate 404 probe).
    Absent,
    /// The member's presence is unknown (any other status, incl. every indeterminate one) —
    /// never coerced to `Absent`.
    Unknown,
}

/// Derive a probed member's [`Membership`] from the wire status (INV-1): only a 200 proves
/// presence and only a 404 proves absence; every other status (crucially every indeterminate
/// one) is `Unknown`, never a fabricated absence.
#[must_use]
pub fn membership(status: u16) -> Membership {
    match status {
        200 => Membership::Present,
        404 => Membership::Absent,
        _ => Membership::Unknown,
    }
}

fn dir_f(op: DirOpKind) -> &'static str {
    match op {
        DirOpKind::Create => "add",
        DirOpKind::Delete => "remove",
        DirOpKind::Probe => "contains",
    }
}

/// The `:value` on a directory **invoke**: a create/delete carries the member string; a probe
/// carries `[member nil]` (the membership result is unknown at invoke time).
fn dir_invoke_value(op: DirOpKind, member: &str) -> String {
    match op {
        DirOpKind::Create | DirOpKind::Delete => format!("\"{member}\""),
        DirOpKind::Probe => format!("[\"{member}\" nil]"),
    }
}

/// The `:value` on a directory **completion**: a create/delete echoes the member; a probe
/// carries `[member present?]` where `present?` is `true`/`false`/`nil` per [`membership`] —
/// an indeterminate probe is `[member nil]`, never `[member false]` (INV-1).
fn dir_completion_value(op: DirOpKind, member: &str, status: u16) -> String {
    match op {
        DirOpKind::Create | DirOpKind::Delete => format!("\"{member}\""),
        DirOpKind::Probe => {
            let present = match membership(status) {
                Membership::Present => "true",
                Membership::Absent => "false",
                Membership::Unknown => "nil",
            };
            format!("[\"{member}\" {present}]")
        }
    }
}

/// Map a directory op's wire status to its completion `:type` (INV-1): an **indeterminate**
/// status is `:info`; a determinate membership response (probe 200/404) or a determinate
/// mutation success is `:ok`; any other determinate status is `:fail`.
fn dir_completion_type(op: DirOpKind, status: u16) -> CompletionType {
    if is_indeterminate(status) {
        return CompletionType::Info;
    }
    let ok = match op {
        DirOpKind::Create => is_success(status),
        DirOpKind::Delete => is_success(status) || status == 404,
        DirOpKind::Probe => status == 200 || status == 404,
    };
    if ok {
        CompletionType::Ok
    } else {
        CompletionType::Fail
    }
}

// ─── Verdict-dispatch seam (mirrors xtask/src/metadata_faults.rs) ──────────────────────

/// The off-Check job that runs the recognized checker (**Elle**) over the serialized history
/// — the privileged leg (JVM/Clojure) ADR-0041/ADR-0016 keep OUT of `cargo xtask ci`.
pub const ELLE_OFF_CHECK_VERDICT_JOB: &str = "elle-register-verdict";

/// A deprecated in-gate shell-out env var. There is **no** in-gate Elle verdict — the checker
/// is JVM/Clojure and stays off-Check; this is representable only so the routing decision is
/// testable, and selecting it is a hard error downstream, never a JVM shell-out into `ci`
/// (the same shape `metadata_faults.rs` keeps for its removed external command).
pub const ELLE_IN_GATE_CMD_VAR: &str = "WYRD_ELLE_IN_GATE_CMD";

/// Where the register/namespace linearizability **verdict** routes. Modelling the route as a
/// value with BOTH alternatives representable is the non-tautological bar (mirrors
/// `xtask::metadata_faults::MetadataTierDispatch`): a Check-time unit test binds to
/// [`consistency_verdict_dispatch`], and a regression that re-points the default route at the
/// (nonexistent) in-gate JVM shell-out flips that test **red behaviourally**.
#[derive(Debug, PartialEq, Eq)]
pub enum ConsistencyVerdictDispatch {
    /// Run the recognized checker (Elle) over the serialized history in the privileged
    /// off-Check `job` — the ADR-0041 default.
    OffCheckElle {
        /// The off-Check verdict job name.
        job: &'static str,
    },
    /// The removed in-gate shell-out to `env_var` — representable but never re-selected for the
    /// default inputs; the runner turns it into a hard error rather than shelling a JVM into
    /// `cargo xtask ci`.
    InGateShellOut {
        /// The deprecated env var that would (illegitimately) select an in-gate shell-out.
        env_var: &'static str,
    },
}

/// Decide where the consistency verdict routes. Pure — decided solely from
/// `in_gate_shellout_configured` (whether the deprecated [`ELLE_IN_GATE_CMD_VAR`] is set) — so
/// the dispatch test binds to it without a privileged environment. The default (var unset) is
/// **always** the off-Check Elle job (ADR-0041/ADR-0016): the in-gate merge gate stays
/// pure-Rust and JVM-free.
#[must_use]
pub fn consistency_verdict_dispatch(
    in_gate_shellout_configured: bool,
) -> ConsistencyVerdictDispatch {
    if in_gate_shellout_configured {
        ConsistencyVerdictDispatch::InGateShellOut {
            env_var: ELLE_IN_GATE_CMD_VAR,
        }
    } else {
        ConsistencyVerdictDispatch::OffCheckElle {
            job: ELLE_OFF_CHECK_VERDICT_JOB,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(nanos: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(nanos)
    }

    fn op(kind: OpKind, key: &str, version: Option<u64>, status: u16, s: u64, e: u64) -> OpRecord {
        OpRecord {
            kind,
            key: key.to_string(),
            version,
            status,
            start: at(s),
            end: at(e),
        }
    }

    #[test]
    fn indeterminate_predicate_flags_5xx_and_synthetic_zero() {
        assert!(is_indeterminate(0));
        assert!(is_indeterminate(500));
        assert!(is_indeterminate(503));
        assert!(!is_indeterminate(200));
        assert!(!is_indeterminate(404));
        assert!(!is_indeterminate(403));
    }

    #[test]
    fn membership_never_fabricates_absence() {
        assert_eq!(membership(200), Membership::Present);
        assert_eq!(membership(404), Membership::Absent);
        assert_eq!(membership(503), Membership::Unknown);
        assert_eq!(membership(0), Membership::Unknown);
    }

    #[test]
    fn indeterminate_put_is_info_not_ok() {
        assert_eq!(register_completion_keyword(OpKind::Put, 500), "info");
        assert_eq!(register_completion_keyword(OpKind::Put, 200), "ok");
        assert_eq!(register_completion_keyword(OpKind::Get, 404), "ok");
    }

    #[test]
    fn verdict_routes_off_check_by_default() {
        assert_eq!(
            consistency_verdict_dispatch(false),
            ConsistencyVerdictDispatch::OffCheckElle {
                job: ELLE_OFF_CHECK_VERDICT_JOB
            }
        );
    }

    #[test]
    fn cross_key_read_write_overlap_is_not_genuine_concurrency() {
        let h = MultiProcessHistory::from_ops(vec![
            ProcOp {
                process: 1,
                record: op(OpKind::Get, "a", Some(1), 200, 0, 10),
            },
            ProcOp {
                process: 0,
                record: op(OpKind::Put, "b", Some(1), 200, 5, 15),
            },
        ]);
        assert!(!h.is_genuinely_concurrent());
    }
}
