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
//!    [`DirectoryHistory::to_elle_edn`]) in the **vocabulary elle-cli 0.1.9 actually accepts**
//!    (verified at Plan against the real jar, #408): the register history in the
//!    `rw-register` **transaction-micro-op** form (`:f :txn`, `:value [[:w key v]]` /
//!    `[[:r key v]]`) and the directory in the `set` model (`:add` of integer elements + one
//!    composed `:read`); every **indeterminate** wire outcome maps to `:info`, never a definite
//!    `:ok`/`:fail` (INV-1);
//! 4. **session** read-your-writes + monotonic-read checks and a **per-key** read-monotonicity
//!    check, all *sound* — an indeterminate op never establishes a definite obligation and is
//!    never counted as a violation (INV-1);
//! 5. a **directory-as-set** history ([`DirectoryHistory`]) in Elle's `set` vocabulary — create
//!    `:add`s of unique **integer** elements during the fault window plus ONE composed post-heal
//!    `:read` of the present set ([`compose_final_read`], Design §2); deletes and mid-run probes
//!    run for the Wyrd-side counts but, by construction, never enter the set EDN
//!    (`:remove`/`:contains` are checker-rejected — verified `:unknown` at Plan); an indeterminate
//!    create is `:info`, and a sweep that cannot resolve every member degrades the composed read
//!    to `:info` rather than omit the member from a definite `:ok` set;
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
//! completion-type (see [`register_completion_keyword`]); the directory set-`:add` completion-type
//! and the composed-read membership derivation (see [`membership`] and [`compose_final_read`] —
//! an unresolved member degrades the composed read to `:info` instead of being silently dropped
//! from a definite `:ok` set, which would state a fabricated **absence**); the RYW PUT/DELETE
//! obligation arms and the RYW
//! read side (see [`MultiProcessHistory::session_read_your_writes`]); and the monotonic-read
//! checks (see [`MultiProcessHistory::session_monotonic_reads`] and
//! [`MultiProcessHistory::reads_monotone_per_key`]).
//!
//! INV-1 has a **dual** the delete pool's construction turns on: no function may fabricate a
//! definite *violation* either. The checks compare client-assigned version tags, which order by
//! commit only when a key has ONE writer — so the pool keys are built single-writer-per-key
//! ([`delete_pool_key`]) rather than the checks being loosened.
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

// ─── Pool construction (Design §2) — the keys, and why they are shaped this way ────────

/// The **Elle-fed register overwrite pool**'s single shared key (Design §2). A shared key is
/// correct *here* — and only here — because the pool has exactly **one writer**: the overwrite
/// process assigns a unique, ascending version per write, and the concurrent reader only reads.
/// It is the key the INV-2 concurrency witness and the `rw-register` verdict are claimed on.
pub const REGISTER_OVERWRITE_POOL_KEY: &str = "checked-register";

/// The **Wyrd-checked delete pool**'s key for `process` — **one writer per key, by
/// construction** (Design §2).
///
/// # Why per-process keys, and not one shared key with disjoint version bands
///
/// The register version this workload writes is **client-assigned**
/// ([`crate::consistency_observable::ObservableS3Client::put`] takes it as an argument) and a read
/// observes the tag its writer chose. So a version tag orders by **writer**, not by **commit**.
/// All three #406 checks that judge this pool
/// ([`MultiProcessHistory::session_read_your_writes`],
/// [`MultiProcessHistory::session_monotonic_reads`], [`MultiProcessHistory::reads_monotone_per_key`])
/// compare raw version numbers on a key and assume tag order **is** commit order — which holds iff
/// a key has a single writer.
///
/// Give two processes disjoint *bands* on one shared key (`p0: 1..`, `p1: 1_000_000..`) and that
/// assumption breaks: the linearizable execution
/// `p0 PUT v=1; p1 PUT v=1_000_001; p1 GET→1_000_001; p0 PUT v=2; p0 GET→2; p1 GET→2`
/// commits `v=2` *after* `v=1_000_001`, so a later read legitimately observes the **smaller** tag
/// and every one of the three checks reports `false` — a **fabricated violation** on a correct
/// system, which the runner escalates to "a real violation observed on the live cluster". That is
/// INV-1's prohibition in the other direction (fabricated certainty *of a fault*), and it would
/// wreck the credibility artifact this issue exists to produce.
///
/// Disjoint keys remove the premise instead of patching the symptom: one writer per key ⇒ tag
/// order = commit order ⇒ the checks are sound as landed, untouched (they stay out of scope).
/// Cross-process traffic still exists **where a version comparison is actually sound** — the
/// Elle-fed overwrite pool, where the real checker judges concurrency. Pinned at Check by
/// `crates/server/tests/consistency_workload.rs`
/// (`delete_pool_keys_are_single_writer_per_key_so_version_tags_track_commit_order`).
#[must_use]
pub fn delete_pool_key(process: usize) -> String {
    format!("checked-delete-register-p{process}")
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
                            // It must NOT erase a standing committed AtLeast — the register version
                            // only climbs, so a floor from an earlier *determinate* write still
                            // holds whether or not this uncertain write took effect (a later read
                            // below it is a real read-your-writes violation). It MAY relax a
                            // standing Absent, since the write may have (re)created the key.
                            if matches!(obligation.get(key), Some(Obligation::Absent(_))) {
                                obligation.insert(key, Obligation::Unknown);
                            }
                        } else if is_success(status) {
                            obligation.insert(
                                key,
                                Obligation::AtLeast(
                                    op.record.version.unwrap_or(0),
                                    op.record.start,
                                ),
                            );
                        }
                        // A determinate-failed PUT had no effect: the obligation is unchanged.
                    }
                    OpKind::Delete => {
                        if is_indeterminate(status) {
                            // INV-1: an indeterminate delete establishes no definite absence. It
                            // must NOT erase a standing Absent (a delete can't resurrect a key). It
                            // MAY relax a standing AtLeast, since the delete may have removed the
                            // committed value, so a later absent/older read is not provably a loss.
                            if !matches!(obligation.get(key), Some(Obligation::Absent(_))) {
                                obligation.insert(key, Obligation::Unknown);
                            }
                        } else if is_success(status) || status == 404 {
                            obligation.insert(key, Obligation::Absent(op.record.start));
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
                                // Resurrected after our own delete — but only a violation if no
                                // OTHER process could have recreated the key AFTER that delete (a
                                // concurrent PUT that can linearize between our delete and this read
                                // legitimately explains it).
                                Obligation::Absent(deleted_at)
                                    if !self.key_recreated_between(
                                        key,
                                        op.process,
                                        deleted_at,
                                        op.record.end,
                                    ) =>
                                {
                                    return false;
                                }
                                Obligation::AtLeast(w, _) if v < w => return false, // read older than own write
                                _ => {}
                            },
                            // `None` version means only "status != 200", which is a definite
                            // absence ONLY for a determinate 404. Any other determinate non-200
                            // read (403/409/412/416/…) observed nothing about the register, so it
                            // must NOT be counted an own-write-lost (INV-1: no fabricated absence).
                            None => {
                                // A 404 after our own write is own-write-lost — unless another
                                // process could have deleted the key AFTER that write (which
                                // legitimately explains the absence).
                                if let Obligation::AtLeast(_, written_at) = obl {
                                    if status == 404
                                        && !self.key_deleted_between(
                                            key,
                                            op.process,
                                            written_at,
                                            op.record.end,
                                        )
                                    {
                                        return false; // own write lost: read absent (404) after writing
                                    }
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

    /// Serialize to **Elle's EDN `rw-register` transaction-micro-op format** — the vocabulary
    /// elle-cli 0.1.9 actually accepts (verified at Plan against the real jar; the #406 scalar
    /// `:value` shape was rejected with "Don't know how to create ISeq from: java.lang.Long").
    /// One `:invoke` entry at each op's `start` and one completion (`:ok` / `:fail` / `:info`) at
    /// its `end`, the whole flat log sorted by relative time. Each entry is
    /// `{:process P, :type T, :f :txn, :value [[<micro-op>]], :time N}` where the micro-op is
    /// `[:w <key> <int>]` for a write and `[:r <key> <int-or-nil>]` for a read — the register key
    /// lives **inside** the micro-op (elle partitions per key on it), so no separate `:key` field
    /// is emitted. INV-1: an **indeterminate** completion is `:info` (never a definite
    /// `:ok`/`:fail`), see [`register_completion_keyword`].
    ///
    /// **Exclusion by construction (Design §2), never per-op filtering.** The Elle-fed overwrite
    /// pool is built PUT/GET-only: a register DELETE has no faithful `rw-register` encoding (a
    /// nil-write `[:w k nil]` makes a *correct* history come back `false`; a 404-after-delete read
    /// maps to `nil`, indistinguishable from unwritten), so deletes belong to the disjoint
    /// Wyrd-checked delete pool and are **never** serialized here. Reaching this function with a
    /// DELETE (or a versionless write) is a pool-construction bug, not something to silently drop —
    /// it panics rather than emit a checker-rejected `[:w k nil]`.
    ///
    /// `:time` is nanoseconds relative to the earliest `start` in the history (Jepsen's
    /// test-relative clock), so the bytes are stable and small.
    ///
    /// This in-gate serialization proves the serializer is **stable and well-shaped**; the
    /// committed real-elle-cli-accepted golden fixtures (`xtask/tests/fixtures/consistency-run/`)
    /// pin the vocabulary against the actual checker, and the deferred off-Check verdict leg
    /// (ADR-0041) runs the real jar over the SAME serialized history.
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
            let key = &op.record.key;
            let (invoke_value, completion_value) = match op.record.kind {
                OpKind::Put => {
                    let w = register_write_microop(key, op.record.version);
                    (w.clone(), w)
                }
                OpKind::Get => (
                    register_read_microop(key, None),
                    register_read_microop(key, op.record.version),
                ),
                OpKind::Delete => panic!(
                    "a register DELETE has no faithful rw-register (txn) encoding — the Elle-fed \
                     overwrite pool must be constructed delete-free (Design §2); deletes belong to \
                     the disjoint Wyrd-checked delete pool and are never serialized here"
                ),
            };
            entries.push(Entry {
                time: rel_nanos(base, op.record.start),
                process: op.process,
                phase: Phase::Invoke,
                rendered: render_entry(
                    op.process,
                    "invoke",
                    "txn",
                    None,
                    &invoke_value,
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
                    "txn",
                    None,
                    &completion_value,
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

    /// Could a process OTHER than `reader` have (re)created `key` in the window between the
    /// reader's own DELETE (which started at `deleted_at`) and this read (completing at
    /// `read_end`)? Such a cross-process PUT can linearize after the delete and before the read, so
    /// a present read after our own delete is not provably a resurrection — another process
    /// legitimately recreated the key (INV-1: never fabricate a violation). The candidate PUT must
    /// be able to linearize BOTH after our delete (`end >= deleted_at`, i.e. not strictly before
    /// it) AND before this read (`start <= read_end`); a PUT ordered entirely before the delete
    /// cannot recreate the key, so it does not waive the violation.
    fn key_recreated_between(
        &self,
        key: &str,
        reader: usize,
        deleted_at: SystemTime,
        read_end: SystemTime,
    ) -> bool {
        self.ops.iter().any(|o| {
            o.process != reader
                && o.record.kind == OpKind::Put
                && is_success(o.record.status)
                && o.record.key.as_str() == key
                && o.record.end >= deleted_at
                && o.record.start <= read_end
        })
    }

    /// The delete mirror of [`key_recreated_between`](Self::key_recreated_between): could a process
    /// OTHER than `reader` have removed `key` between the reader's own PUT (which started at
    /// `written_at`) and this read? If so, a 404 read after our own write is not provably an
    /// own-write-lost. The candidate DELETE must be able to linearize both after our write
    /// (`end >= written_at`) and before this read (`start <= read_end`).
    fn key_deleted_between(
        &self,
        key: &str,
        reader: usize,
        written_at: SystemTime,
        read_end: SystemTime,
    ) -> bool {
        self.ops.iter().any(|o| {
            o.process != reader
                && o.record.kind == OpKind::Delete
                && (is_success(o.record.status) || o.record.status == 404)
                && o.record.key.as_str() == key
                && o.record.end >= written_at
                && o.record.start <= read_end
        })
    }
}

/// The standing per-key obligation a session's determinate ops establish. Each mutation-derived
/// obligation carries the `start` of the op that established it, so a cross-process waiver can
/// require the other process's op to be able to linearize AFTER it (not merely before the read).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Obligation {
    /// No determinate obligation stands (including after an *indeterminate* mutation, whose
    /// effect is unknown — INV-1).
    Unknown,
    /// A determinate PUT of this version stands (established at the given time): a later
    /// determinate read must observe ≥ it.
    AtLeast(u64, SystemTime),
    /// A determinate DELETE stands (established at the given time): a later determinate read must
    /// observe absence.
    Absent(SystemTime),
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

/// `a` strictly precedes `b` in real time — its span ends before `b`'s begins, so the two are
/// genuinely ordered. Overlapping (or endpoint-touching) spans are NOT ordered: either can
/// linearize first.
fn strictly_before(a: &OpRecord, b: &OpRecord) -> bool {
    a.end < b.start
}

fn is_success(status: u16) -> bool {
    (200..300).contains(&status)
}

/// Determinate-read monotonicity per key (INV-1): an indeterminate GET and an absent (404) read
/// establish no version to compare. A present read regresses only against another present read on
/// the same key that **strictly precedes it in real time** yet observed a newer version — two
/// *overlapping* reads have no real-time order (either can linearize first), so they never force a
/// monotonic comparison, and a valid concurrent execution is not rejected.
fn reads_are_monotone<'a>(ops: impl Iterator<Item = &'a ProcOp>) -> bool {
    let reads: Vec<&ProcOp> = ops
        .filter(|op| {
            op.record.kind == OpKind::Get
                && !is_indeterminate(op.record.status)
                && op.record.version.is_some()
        })
        .collect();
    for b in &reads {
        let bv = b.record.version.expect("filtered to Some");
        for a in &reads {
            let av = a.record.version.expect("filtered to Some");
            if a.record.key == b.record.key && av > bv && strictly_before(&a.record, &b.record) {
                return false; // an earlier, real-time-ordered read saw a newer version
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

/// Render one operation-history map: `{:process P, :type :T, :f :F, :value V, :time N}`, with an
/// optional `:key`. Both current serializers pass `key: None`: the register `rw-register` form
/// carries its key **inside** the txn micro-op (`[:w key v]` / `[:r key v]`, where the checker
/// partitions per key), and the `set` form carries its integer element in `:value` — so neither
/// needs a separate `:key` field. The parameter is retained for callers that key a flat register
/// entry directly.
fn render_entry(
    process: usize,
    type_kw: &str,
    f: &str,
    key: Option<&str>,
    value: &str,
    time: u128,
) -> String {
    match key {
        Some(k) => format!(
            "{{:process {process}, :type :{type_kw}, :f :{f}, :key {k}, :value {value}, :time {time}}}"
        ),
        None => {
            format!("{{:process {process}, :type :{type_kw}, :f :{f}, :value {value}, :time {time}}}")
        }
    }
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

/// A string as a quoted, escaped EDN string literal. Directory members are object names, which
/// can contain `"`, `\`, or control characters; rendered raw into EDN they would produce invalid
/// or corrupted checker input, so escape them the way EDN/Clojure readers expect.
fn edn_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The write micro-op `[[:w <key> <int>]]` (Design §3). A register write ALWAYS carries a
/// version — the overwrite pool assigns a unique one per write — so a `None` version is a
/// pool-construction bug: emitting `[:w k nil]` was verified at Plan to make even a *correct*
/// history come back `false`, so this panics rather than fabricate the checker-rejected shape.
fn register_write_microop(key: &str, version: Option<u64>) -> String {
    let v = version.expect(
        "a register write must carry a version — a nil-write [:w k nil] is a checker-rejected \
         fabrication (Design §3); the overwrite pool assigns a unique version per write",
    );
    format!("[[:w {} {v}]]", edn_string(key))
}

/// The read micro-op `[[:r <key> <int-or-nil>]]` (Design §3): the invoke side passes `None`
/// (`nil` — the result is unknown at invoke time), the completion side passes the observed
/// version (`nil` for an absent/unwritten read).
fn register_read_microop(key: &str, version: Option<u64>) -> String {
    format!("[[:r {} {}]]", edn_string(key), edn_version(version))
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

// ─── Directory-as-set history (Elle `set` model) ──────────────────────────────────────

/// A directory member's derived presence from a membership-probe status (INV-1):
/// `200 → Present`, `404 → Absent`, **everything else → Unknown** — never definitely-absent.
/// Used to compose the post-heal final read (Design §2): only a determinate `Present` member
/// enters the composed `:read` set; an `Unknown` probe never fabricates presence OR absence.
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

/// One create in the directory **set** model (Design §2): an `add` of a unique **integer**
/// element `id` during the fault window. The name↔id map lives in the run summary/report — Elle's
/// `set` checker requires integer elements (verified at Plan: string elements crash the valid
/// case to `:unknown`), so the wire object name never enters the EDN.
#[derive(Debug, Clone, Copy)]
pub struct DirCreate {
    /// The client process id that issued the create.
    pub process: usize,
    /// The unique integer element added (the `:value` of the `:add`).
    pub id: u64,
    /// The HTTP status the create returned (200/204/5xx/…).
    pub status: u16,
    /// Real time just before the create began.
    pub start: SystemTime,
    /// Real time just after the create response was read.
    pub end: SystemTime,
}

/// The single **composed post-heal full-set read** (Design §2, Jepsen's own final-read pattern):
/// after heal + quiesce the scenario probes every member of the known universe sequentially and
/// emits ONE `:read` of the whole present set — sound because the set is no longer mutating.
/// `present` is the integer elements observed present (composed from determinate 200 probes via
/// [`membership`]); a mid-run single-member probe can never compose an atomic set read, so only
/// this one exists in the EDN.
///
/// # `unresolved` is what keeps the composed read honest (INV-1)
///
/// A composed `:ok` read is a claim about the **whole** set: "these elements are present and every
/// other created element is *not*". So a member whose probe came back [`Membership::Unknown`]
/// (5xx/timeout — exactly what a nemesis induces) cannot simply be left out of `present`: in the
/// `set` model an acknowledged `:add` missing from a definite final read **is a lost element**, and
/// the real checker returns `false` for it (verified — the committed
/// `directory-history-known-bad.edn` fixture is precisely that shape). Silently omitting an unknown
/// member would therefore fabricate a violation out of an unanswered question.
///
/// Unknown members are recorded here instead, which makes the composed read **indeterminate**
/// ([`is_determinate`](Self::is_determinate)) and serializes it as `:info` with a `nil` value
/// rather than a definite `:ok` — the same "may or may not have happened" the register side gives
/// an indeterminate op. Verified against elle-cli 0.1.9: a `set` history whose final read is
/// `:info` (or absent) comes back **`:unknown`**, not a vacuous `true` — so the honest degrade is
/// enforced by the checker itself and lands the run in INCONCLUSIVE, never a silent pass.
#[derive(Debug, Clone)]
pub struct DirFinalRead {
    /// The client process id that performed the composed final read.
    pub process: usize,
    /// The integer elements observed **present** (determinate 200 probes) in the sweep.
    pub present: Vec<u64>,
    /// The integer elements whose presence the sweep could **not** determine ([`Membership::Unknown`]
    /// — every indeterminate status). Non-empty ⇒ the composed read is `:info`, never a definite
    /// `:ok` that would read as "these members are absent".
    pub unresolved: Vec<u64>,
    /// The real time the composed sweep began.
    pub start: SystemTime,
    /// The real time the composed sweep completed.
    pub end: SystemTime,
}

impl DirFinalRead {
    /// Whether the sweep resolved **every** probed member, so the composed read is a definite
    /// claim about the whole set (`:ok`). An unresolved member makes it `:info` (INV-1).
    #[must_use]
    pub fn is_determinate(&self) -> bool {
        self.unresolved.is_empty()
    }
}

/// **Compose the post-heal full-set read** from the swept universe (Design §2) — the decision half
/// of the sweep, kept here in the library (rather than in the `fdb`-gated live scenario) so the
/// rule below is exercised by an ordinary `cargo xtask ci` unit test instead of only on a
/// privileged cluster. `probes` is the `(element id, probe status)` of every member of the known
/// universe, in sweep order; the scenario does the I/O (and any re-probing) and hands the outcomes
/// here.
///
/// The classification is [`membership`]'s, and each branch is a distinct claim (INV-1):
/// * `Present` (determinate 200) — the element **is** in the set: it enters `present`.
/// * `Absent` (determinate 404) — the element is **not** in the set: it is genuinely excluded. If
///   its create was acknowledged, that is a real lost element and the checker MUST see it as one;
///   this is the one exclusion that is an observation rather than an absence of one.
/// * `Unknown` (anything else) — nothing was observed: it enters `unresolved`, degrading the whole
///   composed read to `:info`. Omitting it from a definite `:ok` read would state "absent", which
///   is a fabricated observation and, in the `set` model, a fabricated *violation*.
#[must_use]
pub fn compose_final_read(
    process: usize,
    probes: &[(u64, u16)],
    start: SystemTime,
    end: SystemTime,
) -> DirFinalRead {
    let mut present = Vec::new();
    let mut unresolved = Vec::new();
    for (id, status) in probes {
        match membership(*status) {
            Membership::Present => present.push(*id),
            Membership::Absent => {}
            Membership::Unknown => unresolved.push(*id),
        }
    }
    DirFinalRead {
        process,
        present,
        unresolved,
        start,
        end,
    }
}

/// A recorded directory-as-set history in Elle's **`set`** vocabulary — `:add` of integer
/// elements plus ONE composed `:read` of the present set. Deletes and mid-run membership probes
/// run for the Wyrd-side counts and the concurrency witness but, by construction (Design §2),
/// **never** enter this history: the `set` checker knows only `:add`/`:read` (verified at Plan —
/// `:remove`/`:contains` come back `:unknown`), so exclusion is at pool construction, never a
/// per-op filter over a mixed history.
#[derive(Debug, Default, Clone)]
pub struct DirectoryHistory {
    creates: Vec<DirCreate>,
    final_read: Option<DirFinalRead>,
}

impl DirectoryHistory {
    /// Build a set-model directory history from the create ops and the composed final read
    /// (Design §2). Passing `None` for the final read yields an add-only history (the pre-heal
    /// partial view a test may pin); the live run always supplies the composed read.
    #[must_use]
    pub fn from_set_run(creates: Vec<DirCreate>, final_read: Option<DirFinalRead>) -> Self {
        Self {
            creates,
            final_read,
        }
    }

    /// The recorded `:add` create ops, in order.
    #[must_use]
    pub fn creates(&self) -> &[DirCreate] {
        &self.creates
    }

    /// The composed post-heal final read, if one was recorded.
    #[must_use]
    pub fn final_read(&self) -> Option<&DirFinalRead> {
        self.final_read.as_ref()
    }

    /// Serialize to Elle's **`set`** vocabulary (Design §3, verified against elle-cli 0.1.9): a
    /// `{:f :add, :value <int>}` invoke/completion pair per create (INV-1: an indeterminate create
    /// is `:info`), then — if a composed read was recorded — ONE `{:f :read, :value #{<ints>}}`
    /// completion carrying the present set. **Integer elements only**, no `:remove`/`:contains`.
    #[must_use]
    pub fn to_elle_edn(&self) -> String {
        if self.creates.is_empty() && self.final_read.is_none() {
            return "[]".to_string();
        }
        let base = self
            .creates
            .iter()
            .map(|c| c.start)
            .chain(self.final_read.iter().map(|r| r.start))
            .min()
            .unwrap_or(UNIX_EPOCH);
        let mut entries: Vec<Entry> = Vec::with_capacity(self.creates.len() * 2 + 2);
        for c in &self.creates {
            let value = c.id.to_string();
            entries.push(Entry {
                time: rel_nanos(base, c.start),
                process: c.process,
                phase: Phase::Invoke,
                rendered: render_entry(
                    c.process,
                    "invoke",
                    "add",
                    None,
                    &value,
                    rel_nanos(base, c.start),
                ),
            });
            entries.push(Entry {
                time: rel_nanos(base, c.end),
                process: c.process,
                phase: Phase::Complete,
                rendered: render_entry(
                    c.process,
                    set_add_completion_type(c.status).keyword(),
                    "add",
                    None,
                    &value,
                    rel_nanos(base, c.end),
                ),
            });
        }
        if let Some(read) = &self.final_read {
            entries.push(Entry {
                time: rel_nanos(base, read.start),
                process: read.process,
                phase: Phase::Invoke,
                rendered: render_entry(
                    read.process,
                    "invoke",
                    "read",
                    None,
                    "nil",
                    rel_nanos(base, read.start),
                ),
            });
            // INV-1: a sweep that could not resolve every member observed no definite set, so the
            // composed read completes `:info` with a `nil` value — never an `:ok` of the partial
            // set, which would claim the unresolved members are ABSENT (a lost element in the
            // `set` model, i.e. a fabricated violation). Verified against elle-cli 0.1.9: this
            // makes the model return `:unknown` ⇒ INCONCLUSIVE, never a silent pass.
            let (completion, value) = if read.is_determinate() {
                ("ok", edn_int_set(&read.present))
            } else {
                ("info", "nil".to_string())
            };
            entries.push(Entry {
                time: rel_nanos(base, read.end),
                process: read.process,
                phase: Phase::Complete,
                rendered: render_entry(
                    read.process,
                    completion,
                    "read",
                    None,
                    &value,
                    rel_nanos(base, read.end),
                ),
            });
        }
        render_history(entries)
    }

    /// The number of **history ops** this directory history contains — the honest "history size"
    /// for the report (Design §6): one op per create, plus ONE for the composed final read.
    ///
    /// This is deliberately not "creates + members swept". The post-heal sweep probes every member
    /// of the universe, but those probes are the *raw material* of the composed read, not ops in
    /// the checked history: they enter the EDN as a single `:read` entry. Counting them would
    /// overstate the checked directory history by roughly 2x in the one field an outsider uses to
    /// judge how much was actually checked — and the Success criterion is that the run "refuses to
    /// overstate itself".
    #[must_use]
    pub fn op_count(&self) -> usize {
        self.creates.len() + usize::from(self.final_read.is_some())
    }
}

/// The `set`-model create completion `:type` (INV-1): an **indeterminate** status is `:info`
/// (the create may or may not have added the element), a determinate success is `:ok`, any other
/// determinate status is `:fail`.
fn set_add_completion_type(status: u16) -> CompletionType {
    if is_indeterminate(status) {
        CompletionType::Info
    } else if is_success(status) {
        CompletionType::Ok
    } else {
        CompletionType::Fail
    }
}

/// An EDN integer-set literal `#{<sorted ints>}` (Design §3 — the composed final read's value).
/// Sorted and de-duplicated so the bytes are stable and the set carries each element once.
fn edn_int_set(ids: &[u64]) -> String {
    let mut sorted: Vec<u64> = ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let joined = sorted
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    format!("#{{{joined}}}")
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
