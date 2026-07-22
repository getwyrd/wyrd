//! **Tier-1 Jepsen consistency scenario test** (Milestone 3, proposal 0005 §13.2,
//! `0005:408`; the `xtask` crate touch-point `0005:437-438`).
//!
//! What this test proves that no in-process test can: the **production custodian repair
//! path** (`custodian::reconcile_step` → `reconstruction::reconcile`) upholds the
//! **ADR-0015 consistency contract** over **real gRPC D-server containers** when the
//! cluster is hit by a **crash** (a killed node — `docker kill`) and an **isolation
//! nemesis** on a second node. Two isolation nemeses are covered, in two separate
//! scenario functions (issue #399, ADR-0039's named additive upgrade):
//!
//! - [`jepsen_consistency_over_repair_under_partition_and_crash`] — Jepsen's `:pause`:
//!   a freezer-cgroup process freeze (`docker pause` / `docker unpause`). The isolated
//!   node's own clock **stops**. Cheaper; kept as the existing leg (#250).
//! - [`jepsen_consistency_over_repair_under_live_partition_and_crash`] — Jepsen's
//!   `:partition`: a network-level packet drop (an in-netns `iptables` DROP on the gRPC
//!   port, injected by a `--net container:<isolated>` sidecar) that keeps the container
//!   `running` **and preserves its published-port mapping**. The isolated node stays
//!   **live** on its own clock, network-unreachable, for the fault window, then is
//!   reachable again at the SAME endpoint on heal — the property
//!   [`assert_node_live_during_isolation`] asserts and the freeze leg cannot exhibit.
//!
//! `xtask/src/faults.rs`'s `IsolationNemesis` value (mirroring `jepsen_dispatch`'s
//! born-at-tier pattern, `xtask/src/faults.rs:179`) decides which of the two functions
//! each `cargo xtask jepsen` leg runs.
//!
//! # ADR-0015 contract asserted (`0005:381-403`)
//!
//! 1. **Commit-point-atomic repair under crash** — a crash before the version-conditional
//!    commit leaves the chunk **fully old, never a hybrid** (`0005:385-389`): the
//!    placed-but-uncommitted rebuilt fragment is **collectable garbage** on the spare
//!    server, the committed inode is untouched (victim still in placement — fully old).
//! 2. **Commit-point-atomic repair under partition** — a network partition mid-repair
//!    causes the repair pass to **abort** (transient error, never a partial commit): the
//!    committed inode is unchanged after the aborted pass.
//! 3. **Read-after-commit** — after the repair commits, every server in the committed
//!    placement holds a readable fragment. A committed server that becomes unreadable is
//!    a violation, not a valid state.
//! 4. **Exactly-once convergence** — repair commits exactly once across the partition
//!    heal: the inode version increments by exactly 1 (not 0, not 2+). A second
//!    reconcile step on an empty queue returns Satisfied without re-committing.
//! 5. **Data integrity** — erasure-decoding any K of the N post-repair fragments
//!    reconstructs the original bytes exactly.
//!
//! The RS(6,3) cluster has [`JC_DSERVER_COUNT`] = 10 servers: nine hold the initial
//! N=9 fragments; the tenth (the spare, domain J) receives the rebuilt fragment.
//! Server 0 (domain A) is killed; server 1 (domain B) is partitioned mid-repair.
//!
//! # Production reach
//!
//! The repair trigger is the **sanctioned `enqueue_repair` test stand-in** (the same
//! bridge `tier2_kill_reconstruct.rs:545` uses, `0005:Production-reach`): no production
//! path today enqueues repair for a simply-missing fragment (the missing-fragment
//! detection product gap is a filed follow-on). The `reconcile_step` →
//! `reconstruction::reconcile` path is genuinely traversed against the live cluster —
//! nothing is stubbed after the enqueue.
//!
//! # Gating
//!
//! The test body is `#[ignore]`d — `cargo xtask ci`'s `cargo test --workspace`
//! **compiles and type-checks** this file (proving the harness is real API-bound Rust,
//! not an env-var shell string) without running it. The live execution (`--ignored`)
//! happens only in the privileged off-Check job (`WYRD_TIER1=1`), run by
//! `cargo xtask jepsen`.
//!
//! **Born-at-tier coverage** at Check: the six assertion helpers
//! ([`assert_commit_point_atomic`], [`assert_read_after_commit`],
//! [`assert_exactly_once_convergence`], [`assert_redundancy_outcome`],
//! [`assert_distinct_domains`], [`assert_node_live_during_isolation`]) are regular
//! functions the scenario bodies call; their own non-`#[ignore]`d unit tests run inside
//! `cargo xtask ci`'s `cargo test --workspace`, including **negative controls** (planted
//! anomalies each oracle must CATCH). [`assert_node_live_during_isolation`]'s negative
//! control plants a `"paused"` container state — the falsifiability demonstration for
//! issue #399: if the network-partition leg collapsed back to `docker pause`, this is
//! exactly the (now-caught) input it would produce. Stubbing or removing a helper fails
//! both the unit tests AND the compile-time type-check of the scenario — the
//! born-at-tier seam is load-bearing.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunkstore_grpc::GrpcChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, repair};
use wyrd_custodian::{
    reconcile_step, Custodian, FencedZone, Reconciled, ReconstructionContext, Topology,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

/// Local return type for the assertion helpers: distinct from [`wyrd_traits::Result`]
/// (which has a `BoxError` error type) so the helpers can return a human-readable
/// `String` description and callers can call `.contains()` on the unwrapped error.
type AssertResult = std::result::Result<(), String>;

// ---- Constants ----

/// RS(6,3): k data + m parity = n total fragments per chunk.
const K: usize = 6;
/// RS(6,3): parity fragment count.
const M: usize = 3;
/// RS(6,3): total fragments (K + M = 9).
const N: usize = K + M;
/// Number of D-server containers for the Jepsen consistency cluster:
/// N servers for the initial placement + 1 spare for the re-placed rebuilt fragment.
pub(crate) const JC_DSERVER_COUNT: usize = N + 1;
/// 0-indexed server killed by the crash fault (server 0, failure domain A).
const VICTIM_INDEX: usize = 0;
/// 0-indexed server paused by the network-partition fault (server 1, failure domain B).
const PARTITION_INDEX: usize = 1;
/// 0-indexed spare server that receives the rebuilt fragment (server 9, failure domain J).
const SPARE_INDEX: usize = N; // = 9
/// The inode id used in the test (distinct from the tier2 value of 1).
const INODE_ID: InodeId = 2;
/// The chunk id used in the test — a recognisable sentinel value.
const CHUNK: ChunkId = 0x0001_CAFE_BABE_DEAD;
/// One distinct failure domain per D server. Domains A–J cover servers 0–9.
const DOMAINS: [&str; JC_DSERVER_COUNT] = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];

// ---- In-memory MetadataStore ----
//
// The same shape used by the Tier-0 DST campaign and the Tier-2 sibling scenario
// (`crates/chunkstore-grpc/tests/tier2_kill_reconstruct.rs`). The reconstruction loop
// runs over the `MetadataStore` seam, so this in-memory backend drives the same
// production `reconcile_step` path as the DST — what this scenario adds is running it
// against a REAL gRPC fleet under BOTH crash and partition faults.

#[derive(Default)]
struct MemMeta {
    kv: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (k, v) in batch.puts {
            kv.insert(k, v);
        }
        for k in batch.deletes {
            kv.remove(&k);
        }
        Ok(CommitOutcome::Committed)
    }
}

// ---- Crash-injecting MetadataStore ----
//
// Mirrors `CrashMeta` in `tier2_kill_reconstruct.rs`. While *armed*, it drops the
// reconstruction loop's version-conditional repoint commit (the single batch that
// carries a positive precondition), modelling the custodian dying just before its
// commit lands. The D-server `put_fragment` write (which precedes the commit,
// `0005:277`) is NOT intercepted — it goes through to the real gRPC container — so a
// crash leaves a real placed-but-uncommitted orphan on the spare server.

struct CrashMeta {
    inner: MemMeta,
    armed: AtomicBool,
}

impl CrashMeta {
    fn new() -> Self {
        Self {
            inner: MemMeta::default(),
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Relaxed);
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::Relaxed);
    }
}

#[async_trait]
impl MetadataStore for CrashMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        // The version-conditional repoint is the only commit with a positive precondition
        // (`.require(key, value)`); crash on it (apply nothing, return Conflict) when armed.
        // The intent / enqueue / drain commits carry no positive precondition and pass through.
        if self.armed.load(Ordering::Relaxed)
            && batch.preconditions.iter().any(|p| p.expected.is_some())
        {
            return Ok(CommitOutcome::Conflict);
        }
        self.inner.commit(batch).await
    }
}

// ---- Consistency oracle functions ----
//
// These are regular functions (not `#[cfg(test)]`-only) so the scenario test body calls
// them and the unit tests below are load-bearing: removing or stubbing a helper causes
// the `#[test]` unit tests AND the compile-time type-check of the scenario to fail.
//
// The shared oracle is the same oracle the live scenario asserts with — no decorative
// second oracle that Check exercises but the live run never touches (the iter-8 T2/T4
// finding). The negative controls (planted anomalies) below prove each oracle is
// load-bearing, per ADR-0009.

/// Assert **commit-point-atomicity** after a crash mid-repair (`0005:277`,
/// `0005:385-389`):
///
/// - The placed-but-uncommitted rebuilt fragment **EXISTS** on the spare server
///   (`orphan_on_spare`): the write-before-commit ordering in
///   `reconstruction::repair_chunk` ensures a crash leaves a collectable orphan, not
///   missing bytes.
/// - The committed inode **STILL REFERENCES the victim** (`victim_in_committed_placement
///   == true`): the version-conditional commit never landed, so the inode is at its prior
///   (fully-old) value. If the victim is absent from the committed placement after a crash,
///   the commit partially landed, leaving a torn/hybrid chunk — a violation.
///
/// Returns `Ok(())` when both hold, `Err` describing the first violation otherwise.
pub(crate) fn assert_commit_point_atomic(
    orphan_on_spare: bool,
    victim_in_committed_placement: bool,
) -> AssertResult {
    if !orphan_on_spare {
        return Err(
            "placed-but-uncommitted rebuilt fragment is absent on the spare after a crash; \
             reconstruction writes the fragment BEFORE its commit, so a crash must leave it \
             as collectable garbage — its absence means the write-before-commit ordering \
             was violated"
                .to_string(),
        );
    }
    if !victim_in_committed_placement {
        return Err(
            "the committed inode does NOT reference the victim after a crash mid-repair; \
             the version-conditional commit must NOT have landed — the inode must be fully old \
             (victim still in committed placement), never a torn/hybrid chunk"
                .to_string(),
        );
    }
    Ok(())
}

/// Assert **read-after-commit** (ADR-0015): every server in the `committed_placement`
/// must appear in `readable` — i.e. the fragment at that server is readable after the
/// repair committed. A committed server with no readable fragment is a read-after-commit
/// violation: the product promised the data is there, but it isn't.
///
/// Returns `Ok(())` when all committed servers are readable, `Err` on the first
/// violation.
pub(crate) fn assert_read_after_commit(
    committed_placement: &[DServerId],
    readable: &[DServerId],
) -> AssertResult {
    for &server in committed_placement {
        if !readable.contains(&server) {
            return Err(format!(
                "read-after-commit violation (ADR-0015): server {server} is in the \
                 committed placement but holds no readable fragment; a committed value \
                 must remain readable after commit"
            ));
        }
    }
    Ok(())
}

/// Assert **exactly-once convergence**: the inode version must increase by **exactly
/// 1** across one successful repair. A larger jump means duplicate commits landed; no
/// jump means the commit was lost.
///
/// Returns `Ok(())` when `version_after == version_before + 1`, `Err` otherwise.
pub(crate) fn assert_exactly_once_convergence(
    version_before: u64,
    version_after: u64,
) -> AssertResult {
    let expected = version_before + 1;
    if version_after != expected {
        if version_after == version_before {
            return Err(format!(
                "lost-commit violation: inode version is still {version_before} after repair; \
                 the version-conditional commit must have landed exactly once (expected \
                 version {expected}), but it appears the commit was lost"
            ));
        }
        return Err(format!(
            "duplicate-commit violation: inode version jumped from {version_before} to \
             {version_after} (expected exactly {expected}); repair must commit exactly once, \
             not {} times",
            version_after - version_before
        ));
    }
    Ok(())
}

/// Assert the post-reconstruction redundancy outcome:
/// - exactly `n` fragments are placed (full redundancy restored);
/// - the dead/partitioned server (`dead_server`) is no longer in the placement.
///
/// Returns `Ok(())` when both hold, `Err` describing the first violation otherwise.
pub(crate) fn assert_redundancy_outcome(
    placement: &[DServerId],
    dead_server: DServerId,
    n: usize,
) -> AssertResult {
    if placement.len() != n {
        return Err(format!(
            "expected {n} fragments in placement after reconstruction, got {}; \
             full redundancy not restored",
            placement.len()
        ));
    }
    if placement.contains(&dead_server) {
        return Err(format!(
            "dead server {dead_server} still appears in the post-reconstruction placement; \
             reconstruction must not re-place onto the dead/partitioned server"
        ));
    }
    Ok(())
}

/// Assert that all fragments in `placement` are on **distinct** failure domains,
/// using `domain_of(server_id)` to look up each server's domain label.
///
/// Returns `Ok(())` when all domains are distinct, `Err` on the first violation.
pub(crate) fn assert_distinct_domains<'a, F>(placement: &[DServerId], domain_of: F) -> AssertResult
where
    F: Fn(DServerId) -> Option<&'a str>,
{
    use std::collections::HashSet;
    let mut seen: HashSet<&str> = HashSet::new();
    for &server in placement {
        let domain = domain_of(server).ok_or_else(|| {
            format!(
                "server {server} has no failure-domain assignment in the topology; \
                 all placed servers must be registered"
            )
        })?;
        if !seen.insert(domain) {
            return Err(format!(
                "duplicate failure domain `{domain}` in placement: two fragments are on \
                 the same domain (server {server}), violating the distinct-domain invariant"
            ));
        }
    }
    Ok(())
}

/// Assert **node liveness during isolation** (issue #399, ADR-0039's named additive
/// upgrade) — the property that distinguishes Jepsen's `:partition` nemesis from
/// `:pause`: the isolated node's container must stay in Docker's `running` state
/// (`docker inspect -f '{{.State.Status}}' <container>`) for the whole fault window —
/// it keeps running on its own clock, network-unreachable, rather than being suspended.
///
/// Returns `Ok(())` when `state == "running"`, `Err` describing the violation
/// otherwise. In particular `"paused"` — a freezer-cgroup process freeze (Jepsen's
/// `:pause`) — is the exact collapse this oracle exists to catch: it is the
/// falsifiability negative control for this issue (a stub that routes the
/// network-partition leg to `docker pause` produces exactly this input).
pub(crate) fn assert_node_live_during_isolation(state: &str) -> AssertResult {
    if state == "running" {
        return Ok(());
    }
    if state == "paused" {
        return Err(format!(
            "node-liveness-during-isolation violation: container state is `paused` — a \
             freezer-cgroup process freeze (Jepsen's `:pause`); the network-partition leg \
             must keep the isolated node LIVE (`running`, network-unreachable — Jepsen's \
             `:partition`), not suspend its clock (ADR-0039, issue #399). Got state: {state:?}"
        ));
    }
    Err(format!(
        "node-liveness-during-isolation violation: container state is `{state}`, expected \
         `running` — the isolated node must stay live for the whole fault window"
    ))
}

// ---- Helper unit tests (non-#[ignore], run at Check) ----
//
// These are the born-at-tier coverage for the shared consistency oracle. They run
// inside `cargo xtask ci`'s `cargo test --workspace` without requiring a container
// runtime. Each includes a **negative control** — a planted anomaly the oracle MUST
// catch (per ADR-0009: a born-at-tier oracle must be demonstrated red on a planted
// fault, not just green on the happy path).

// --- assert_commit_point_atomic ---

#[test]
fn commit_point_atomic_passes_when_orphan_exists_and_victim_in_placement() {
    // After a crash before the version-conditional commit: orphan on spare AND victim
    // still in committed placement (fully old inode, commit never landed).
    assert!(
        assert_commit_point_atomic(true, true).is_ok(),
        "orphan present, victim in committed placement: invariant must hold"
    );
}

#[test]
fn commit_point_atomic_fails_when_orphan_absent() {
    // Negative control: no orphan on spare — write-before-commit ordering violated.
    let result = assert_commit_point_atomic(false, true);
    assert!(result.is_err(), "absent orphan must fail the invariant");
    let err = result.unwrap_err();
    assert!(
        err.contains("absent"),
        "error must name the violation: {err}"
    );
}

#[test]
fn commit_point_atomic_fails_when_victim_not_in_committed_placement() {
    // Negative control: victim absent from committed placement after crash — the commit
    // partially applied, leaving a torn/hybrid chunk.
    let result = assert_commit_point_atomic(true, false);
    assert!(
        result.is_err(),
        "victim absent from committed placement after crash must fail"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("hybrid"),
        "error must name the torn/hybrid violation: {err}"
    );
}

// --- assert_read_after_commit ---

#[test]
fn read_after_commit_passes_when_all_committed_servers_are_readable() {
    // All three committed servers also appear in the readable set.
    assert!(
        assert_read_after_commit(&[1, 2, 3], &[0, 1, 2, 3, 4]).is_ok(),
        "all committed servers readable: must pass"
    );
}

#[test]
fn read_after_commit_fails_when_committed_server_is_not_readable() {
    // Negative control (planted anomaly): server 2 is committed but not readable
    // — a post-commit unreadable value, the oracle MUST catch this.
    let result = assert_read_after_commit(&[1, 2, 3], &[1, 3]); // server 2 missing
    assert!(
        result.is_err(),
        "unreadable committed server must fail read-after-commit"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("read-after-commit violation"),
        "error must name the violation: {err}"
    );
    assert!(
        err.contains('2'),
        "error must identify the unreadable server: {err}"
    );
}

// --- assert_exactly_once_convergence ---

#[test]
fn exactly_once_convergence_passes_when_version_increments_by_one() {
    assert!(
        assert_exactly_once_convergence(1, 2).is_ok(),
        "version 1→2: exactly-once convergence"
    );
}

#[test]
fn exactly_once_convergence_fails_on_lost_commit() {
    // Negative control (planted anomaly — lost commit): version unchanged after repair.
    let result = assert_exactly_once_convergence(1, 1);
    assert!(result.is_err(), "unchanged version must fail (lost commit)");
    let err = result.unwrap_err();
    assert!(
        err.contains("lost-commit"),
        "error must name the lost-commit violation: {err}"
    );
}

#[test]
fn exactly_once_convergence_fails_on_duplicate_commit() {
    // Negative control (planted anomaly — duplicate commit): version jumps by 2.
    let result = assert_exactly_once_convergence(1, 3);
    assert!(
        result.is_err(),
        "version jump of 2 must fail (duplicate commit)"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("duplicate-commit"),
        "error must name the duplicate-commit violation: {err}"
    );
}

// --- assert_redundancy_outcome ---

#[test]
fn redundancy_outcome_passes_when_victim_absent_and_n_fragments() {
    assert!(
        assert_redundancy_outcome(&[1, 2, 3], 0, 3).is_ok(),
        "victim 0 absent, 3 fragments: must pass"
    );
}

#[test]
fn redundancy_outcome_fails_when_dead_server_in_placement() {
    // Negative control: dead server 0 still in placement.
    let result = assert_redundancy_outcome(&[0, 1, 2], 0, 3);
    assert!(result.is_err(), "dead server in placement must fail");
    assert!(result.unwrap_err().contains("still appears"));
}

#[test]
fn redundancy_outcome_fails_when_wrong_fragment_count() {
    // Negative control: only 2 fragments, need 3 — withheld repair.
    let result = assert_redundancy_outcome(&[1, 2], 0, 3);
    assert!(result.is_err(), "too few fragments must fail");
    assert!(result.unwrap_err().contains('2'));
}

// --- assert_distinct_domains ---

#[test]
fn distinct_domains_passes_for_all_different() {
    let domain_of =
        |id: DServerId| -> Option<&'static str> { ["A", "B", "C", "D"].get(id as usize).copied() };
    assert!(
        assert_distinct_domains(&[1, 2, 3], domain_of).is_ok(),
        "all distinct domains: must pass"
    );
}

#[test]
fn distinct_domains_fails_for_duplicate_domain() {
    // Negative control: servers 1 and 3 share domain B.
    let domain_of = |id: DServerId| -> Option<&'static str> {
        match id {
            1 => Some("B"),
            2 => Some("C"),
            3 => Some("B"), // duplicate!
            _ => None,
        }
    };
    let result = assert_distinct_domains(&[1, 2, 3], domain_of);
    assert!(result.is_err(), "duplicate domain must fail");
    let err = result.unwrap_err();
    assert!(
        err.contains("duplicate"),
        "error must name the violation: {err}"
    );
    assert!(
        err.contains('B'),
        "error must name the duplicated domain: {err}"
    );
}

#[test]
fn distinct_domains_fails_for_unregistered_server() {
    let domain_of = |_: DServerId| -> Option<&'static str> { None };
    let result = assert_distinct_domains(&[0], domain_of);
    assert!(result.is_err(), "unregistered server must fail");
    assert!(result.unwrap_err().contains("no failure-domain assignment"));
}

// --- assert_node_live_during_isolation (issue #399) ---

#[test]
fn node_liveness_during_isolation_passes_when_running() {
    // The network-partition leg's node stays `running` for the whole fault window —
    // Jepsen's `:partition`, the property distinguishing it from `:pause`.
    assert!(
        assert_node_live_during_isolation("running").is_ok(),
        "a `running` container must pass the node-liveness-during-isolation oracle"
    );
}

#[test]
fn node_liveness_during_isolation_fails_when_paused() {
    // Negative control (the falsifiability demonstration for issue #399): a planted
    // `"paused"` state is EXACTLY what the network-partition leg would produce if it
    // collapsed back to `docker pause` (Jepsen's `:pause`) — the oracle MUST catch it.
    let result = assert_node_live_during_isolation("paused");
    assert!(
        result.is_err(),
        "a `paused` container must fail the node-liveness-during-isolation oracle — a \
         freezer-cgroup process freeze suspends the node's own clock"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("paused") && err.contains(":pause"),
        "error must name the pause/freeze violation: {err}"
    );
}

#[test]
fn node_liveness_during_isolation_fails_when_exited() {
    // Negative control: a container that has stopped entirely also fails — the isolated
    // node must be alive AND unreachable, not simply gone.
    let result = assert_node_live_during_isolation("exited");
    assert!(result.is_err(), "an `exited` container must fail");
    assert!(result.unwrap_err().contains("running"));
}

// ---- Scenario helpers ----

/// Per-request RPC deadline for the cluster clients. Bounds calls to a partitioned
/// (`docker pause`d) node so they fail transiently instead of hanging the scenario — and
/// thus the privileged nightly — until the workflow timeout. Generous enough that a
/// healthy reconstruction read never trips it.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Dial a D server, retrying briefly so a just-launched container that has not yet
/// finished binding its listener is waited on rather than failing the test.
///
/// Uses [`GrpcChunkStore::connect_with_timeout`] so every RPC carries a [`RPC_TIMEOUT`]
/// deadline: a request to the partitioned node returns a transient `DEADLINE_EXCEEDED`
/// rather than hanging — the behaviour Phase 2 (partition mid-repair) asserts on.
async fn connect(endpoint: &str) -> GrpcChunkStore {
    let mut last_err = None;
    for _ in 0..50 {
        match GrpcChunkStore::connect_with_timeout(endpoint.to_string(), RPC_TIMEOUT).await {
            Ok(c) => return c,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("could not connect to D server `{endpoint}`: {last_err:?}");
}

/// Build the failure-domain topology for the cluster, **excluding** the dead `victim`
/// server so the selector never picks it for re-placement.
fn healthy_topology(victim: usize) -> Topology {
    let mut t = Topology::new(vec![]);
    for (i, &domain) in DOMAINS.iter().enumerate() {
        if i != victim {
            t.register(i as DServerId, domain);
        }
    }
    t
}

/// The D-server's in-container gRPC port (the compose service runs
/// `--bind 0.0.0.0:50051`). The live-partition leg DROPs inbound traffic to this port to
/// isolate the node without touching its container state or published-port mapping.
const DSERVER_GRPC_PORT: &str = "50051";

/// Query a container's Docker state (`docker inspect -f '{{.State.Status}}' <container>`)
/// — e.g. `"running"`, `"paused"`, `"exited"`. The live-partition leg
/// ([`jepsen_consistency_over_repair_under_live_partition_and_crash`]) uses this to prove
/// the isolated node stays LIVE (`running`, never `paused`) for the whole fault window —
/// the property [`assert_node_live_during_isolation`] checks (issue #399).
fn docker_container_state(container: &str) -> String {
    let out = std::process::Command::new("docker")
        .args(["inspect", "-f", "{{.State.Status}}", container])
        .output()
        .expect("docker inspect command");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Add or remove (`iptables_verb` = `"-A"` to add, `"-D"` to delete) an in-netns `iptables`
/// DROP rule on `container`'s inbound gRPC port, injected by a throwaway sidecar that
/// **shares the isolated container's network namespace** (`--net container:<container>`)
/// and carries `NET_ADMIN` — so the D-server container itself is never disconnected,
/// paused, or otherwise touched. This is what makes the leg a Jepsen `:partition` (a live
/// node that keeps `running`) rather than `:pause`, AND what fixes the iteration-1 heal
/// failure: because the container never leaves its network, its host-published-port
/// mapping (`127.0.0.1:<host-port>`, which the scenario dials) survives the fault window,
/// so Phase 3 reconnects at the SAME endpoint.
///
/// `image` is the D-server image (`WYRD_TIER1_DSERVER_IMAGE`, `wyrd-dserver:test`), reused
/// here only because it now ships `iptables` — the sidecar overrides its entrypoint.
/// A thin, deliberately untested I/O wrapper (same shape as the `docker kill`/`pause`
/// shell-outs); the pure decision lives in [`assert_node_live_during_isolation`] and
/// `xtask`'s `IsolationNemesis`.
fn docker_netns_grpc_drop(
    image: &str,
    container: &str,
    iptables_verb: &str,
) -> std::process::ExitStatus {
    std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            &format!("--net=container:{container}"),
            "--cap-add=NET_ADMIN",
            "--user=0",
            "--entrypoint=iptables",
            image,
            iptables_verb,
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            DSERVER_GRPC_PORT,
            "-j",
            "DROP",
        ])
        .status()
        .expect("docker run iptables sidecar command")
}

// ---- Scenario test ----

/// **Tier-1 Jepsen consistency campaign** (M3, `0005:408`).
///
/// The `#[ignore]` attribute keeps this body out of `cargo xtask ci` (unprivileged,
/// container-free, ADR-0016); the `#[tokio::test]` attribute ensures `cargo xtask ci`'s
/// `cargo test --workspace` COMPILES and TYPE-CHECKS the body (the "born-at-tier" bar:
/// an API regression on `reconcile_step`/`reconstruction::reconcile` would fail to
/// compile). The body runs only in the privileged `WYRD_TIER1=1` job via
/// `cargo xtask jepsen`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Tier-1: needs real containerized D servers — run via `cargo xtask jepsen`"]
async fn jepsen_consistency_over_repair_under_partition_and_crash() {
    // ---- Read cluster info from env (set by `cargo xtask jepsen`) ----

    let raw_endpoints = match std::env::var("WYRD_DSERVER_ENDPOINTS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier1_jepsen_consistency: WYRD_DSERVER_ENDPOINTS unset — skipping. \
                 Run `cargo xtask jepsen` to stand up the container D servers."
            );
            return;
        }
    };
    let victim_container = match std::env::var("WYRD_TIER1_VICTIM_CONTAINER") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier1_jepsen_consistency: WYRD_TIER1_VICTIM_CONTAINER unset — skipping. \
                 Run `cargo xtask jepsen` to supply the victim container name."
            );
            return;
        }
    };
    let partition_container = match std::env::var("WYRD_TIER1_PARTITION_CONTAINER") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier1_jepsen_consistency: WYRD_TIER1_PARTITION_CONTAINER unset — skipping. \
                 Run `cargo xtask jepsen` to supply the partition container name."
            );
            return;
        }
    };

    let endpoints: Vec<String> = raw_endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    assert_eq!(
        endpoints.len(),
        JC_DSERVER_COUNT,
        "tier1_jepsen_consistency: need exactly {JC_DSERVER_COUNT} endpoints, \
         got {} ({raw_endpoints:?})",
        endpoints.len()
    );

    // ---- Connect to all 10 D servers ----

    let mut clients: Vec<GrpcChunkStore> = Vec::with_capacity(JC_DSERVER_COUNT);
    for endpoint in &endpoints {
        clients.push(connect(endpoint).await);
    }

    // ---- Setup: write RS(6,3) chunk fragments to servers 0–8; spare server 9 holds nothing ----

    // 240 bytes → 40-byte shards under RS(6,3).
    let data: Vec<u8> = (0u8..240).collect();
    let all_shards = erasure::encode(K, M, &data).expect("erasure::encode RS(6,3)");
    assert_eq!(all_shards.len(), N, "encoder must produce N={N} shards");

    // Fragment index `i` → server `i` (identity placement), clients 0–8.
    for (index, shard) in all_shards.iter().enumerate() {
        let fragment_bytes = encode_ec_fragment(CHUNK, index as u16, K as u8, M as u8, shard);
        let frag_id = FragmentId {
            chunk: CHUNK,
            index: index as u16,
        };
        clients[index]
            .put_fragment(frag_id, fragment_bytes)
            .await
            .unwrap_or_else(|e| panic!("put_fragment index {index} to server {index}: {e}"));
    }

    // Create the inode: committed, placement = [0, 1, 2, …, 8], version 1.
    let meta = CrashMeta::new();
    let chunk_ref = ChunkRef {
        id: CHUNK,
        scheme: EcScheme::ReedSolomon {
            k: K as u8,
            m: M as u8,
        },
        len: data.len() as u64,
        placement: (0..N as DServerId).collect(),
    };
    let inode_record = InodeRecord {
        size: data.len() as u64,
        chunk_map: vec![chunk_ref],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let create_outcome = metadata::create(&meta, 0, "test-file", INODE_ID, &inode_record)
        .await
        .expect("metadata::create inode");
    assert_eq!(
        create_outcome,
        CommitOutcome::Committed,
        "inode create must commit"
    );

    // Enqueue a repair obligation for CHUNK (the sanctioned test stand-in, following
    // the tier2_kill_reconstruct.rs:545 precedent — `Production reach` note above).
    repair::enqueue_repair(&meta, CHUNK, "tier1-jepsen-test")
        .await
        .expect("enqueue_repair");

    // ---- Kill server 0: crash fault ----
    //
    // Permanently removes server 0 from the fleet. Fragment 0 is now lost.

    let kill_status = std::process::Command::new("docker")
        .arg("kill")
        .arg(&victim_container)
        .status()
        .expect("docker kill command");
    assert!(
        kill_status.success(),
        "`docker kill {victim_container}` failed (exit {kill_status}); \
         is the victim container name correct?"
    );

    // Build the fleet and topology excluding the killed server.
    // Fleet: servers 1–9 (alive). Topology: servers 1–9, domains B–J.
    let topology = healthy_topology(VICTIM_INDEX);
    let fleet: Vec<(DServerId, &dyn ChunkStore)> = (0..JC_DSERVER_COUNT)
        .filter(|&i| i != VICTIM_INDEX)
        .map(|i| (i as DServerId, &clients[i] as &dyn ChunkStore))
        .collect();

    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "tier1-jepsen-zone")
        .await
        .expect("Custodian::elect");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());

    // ====================================================================
    // Phase 1: crash mid-repair (CrashMeta armed)
    //
    // The reconstruction loop writes the rebuilt fragment to server 9 (a real gRPC
    // put_fragment) THEN attempts the version-conditional commit. CrashMeta intercepts
    // the commit (returns Conflict), simulating the custodian dying just before it
    // lands — exactly `0005:385-386`.
    //
    // Expected: Reconciled::Satisfied (the commit was lost; metadata unchanged).
    // Post-condition: server 9 holds a real placed-but-uncommitted orphan fragment;
    // the committed inode is fully old (victim still at placement[0], version=1).
    // ====================================================================

    meta.arm();
    let phase1_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await
    .expect("phase 1: reconcile_step (crash mid-repair)");

    assert_eq!(
        phase1_outcome,
        Reconciled::Satisfied,
        "phase 1 (crash mid-repair): reconcile_step must return Satisfied \
         when the version-conditional commit is intercepted by CrashMeta"
    );

    // Commit-point-atomic check — property 1 (`0005:385-389`):

    // (a) Placed-but-uncommitted orphan EXISTS on spare server 9:
    let frag0_id = FragmentId {
        chunk: CHUNK,
        index: 0,
    };
    let orphan_bytes = clients[SPARE_INDEX]
        .get_fragment(frag0_id)
        .await
        .expect("get_fragment fragment-0 from spare server after crash");

    // (b) Committed inode is FULLY OLD — version 1, victim still at placement[0]:
    let inode_bytes_p1 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 1")
        .expect("inode must exist after phase 1");
    let inode_p1: InodeRecord =
        metadata::decode(&inode_bytes_p1).expect("decode inode after phase 1");
    assert_eq!(
        inode_p1.version, 1,
        "committed inode must still be at version 1 after crash (no commit landed)"
    );
    assert_eq!(
        inode_p1.chunk_map[0].placement[VICTIM_INDEX], VICTIM_INDEX as DServerId,
        "committed placement[{VICTIM_INDEX}] must still reference the dead server \
         {VICTIM_INDEX} after crash — fully old, never a torn/hybrid chunk"
    );
    assert!(
        !inode_p1.chunk_map[0]
            .placement
            .contains(&(SPARE_INDEX as DServerId)),
        "spare server {SPARE_INDEX} must NOT appear in committed placement after crash; \
         the orphan is collectable garbage, not recorded corruption"
    );

    // Summary oracle assertion — commit-point-atomic:
    let committed_has_victim = inode_p1.chunk_map[0]
        .placement
        .contains(&(VICTIM_INDEX as DServerId));
    assert_commit_point_atomic(orphan_bytes.is_some(), committed_has_victim)
        .expect("commit-point-atomic invariant violated after crash mid-repair");

    // ====================================================================
    // Phase 2: partition mid-repair (server 1 paused — alive-but-unreachable)
    //
    // Injects a NETWORK PARTITION (distinct from the crash): server 1 is alive but
    // unreachable (`docker pause`). The reconstruction path sees server 1 as a
    // TRANSIENT fault (`is_permanent_read_fault` returns false for a connection
    // timeout/refused — `reconstruction.rs:312+`), propagates the error upward, and
    // does NOT commit anything partial.
    //
    // CrashMeta is disarmed: the partition itself is sufficient to abort the pass.
    // Repair obligation remains queued (the aborted pass left it un-drained).
    //
    // Expected: Err (transient fault from partitioned server 1 aborts the pass).
    // Post-condition: committed inode STILL UNCHANGED (version 1, victim in placement).
    // ====================================================================

    meta.disarm();

    let pause_status = std::process::Command::new("docker")
        .arg("pause")
        .arg(&partition_container)
        .status()
        .expect("docker pause command");
    assert!(
        pause_status.success(),
        "`docker pause {partition_container}` failed (exit {pause_status}); \
         is the partition container name correct?"
    );

    let phase2_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await;

    // Phase 2: a transient error from the paused server must abort the pass.
    // No partial commit: ADR-0015 requires that a repair either commits atomically or
    // does nothing — the partition must leave the metadata unchanged.
    assert!(
        phase2_outcome.is_err(),
        "phase 2 (partition mid-repair): reconcile_step must return Err (transient \
         fault from paused server {PARTITION_INDEX} must abort the pass — no partial \
         commit); got: {phase2_outcome:?}"
    );

    // Verify metadata is STILL UNCHANGED after the aborted pass:
    let inode_bytes_p2 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 2")
        .expect("inode must exist after phase 2");
    let inode_p2: InodeRecord =
        metadata::decode(&inode_bytes_p2).expect("decode inode after phase 2");
    assert_eq!(
        inode_p2.version, 1,
        "phase 2: committed inode must still be at version 1 after partition abort; \
         no partial commit must have landed"
    );
    assert!(
        inode_p2.chunk_map[0]
            .placement
            .contains(&(VICTIM_INDEX as DServerId)),
        "phase 2: victim server {VICTIM_INDEX} must still be in committed placement \
         after partition abort — inode fully old"
    );

    // ====================================================================
    // Phase 3: heal partition and converge exactly once
    //
    // Unpause server 1 (heals the partition). The repair obligation is still queued
    // (the aborted pass left it un-drained). With CrashMeta disarmed and server 1
    // alive again, reconcile_step succeeds: rebuilds fragment 0 from fragments 1–8
    // (K=6 survivors, since server 0 is still dead), places the rebuilt fragment on
    // spare server 9 (the only domain distinct from survivors B–I), and commits.
    //
    // Expected: Reconciled::Changed (commit landed, version 1 → 2).
    // ====================================================================

    let unpause_status = std::process::Command::new("docker")
        .arg("unpause")
        .arg(&partition_container)
        .status()
        .expect("docker unpause command");
    assert!(
        unpause_status.success(),
        "`docker unpause {partition_container}` failed (exit {unpause_status})"
    );

    let phase3_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await
    .expect("phase 3: reconcile_step (after partition heal)");

    assert_eq!(
        phase3_outcome,
        Reconciled::Changed,
        "phase 3 (after partition heal): reconcile_step must return Changed \
         (repair committed)"
    );

    // Read the committed inode after phase 3:
    let inode_bytes_p3 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 3")
        .expect("inode must exist after phase 3");
    let inode_p3: InodeRecord =
        metadata::decode(&inode_bytes_p3).expect("decode inode after phase 3");

    // Exactly-once convergence: version must have incremented by exactly 1.
    assert_exactly_once_convergence(1, inode_p3.version)
        .expect("exactly-once convergence violated after phase 3");

    let new_placement = &inode_p3.chunk_map[0].placement;

    // Redundancy outcome: N fragments, dead victim absent.
    assert_redundancy_outcome(new_placement, VICTIM_INDEX as DServerId, N)
        .expect("redundancy outcome violated after reconstruction");

    // Specific placement check: victim slot re-filled with spare server.
    assert_eq!(
        new_placement[VICTIM_INDEX], SPARE_INDEX as DServerId,
        "fragment {VICTIM_INDEX} must be re-placed on spare server {SPARE_INDEX} \
         (the only domain distinct from all survivor domains B–I)"
    );

    // Distinct-domain invariant: all N servers in the new placement on distinct domains.
    assert_distinct_domains(new_placement, |server_id| {
        DOMAINS.get(server_id as usize).copied()
    })
    .expect("distinct-domain invariant violated after reconstruction");

    // ====================================================================
    // Phase 4: exactly-once convergence check
    //
    // Call reconcile_step again. The repair queue was drained by phase 3's commit
    // (the version-conditional commit atomically drains the obligation). A second
    // pass must return Satisfied without committing, and the inode version must be
    // unchanged (still 2).
    // ====================================================================

    let phase4_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await
    .expect("phase 4: reconcile_step (exactly-once check)");

    assert_eq!(
        phase4_outcome,
        Reconciled::Satisfied,
        "phase 4 (exactly-once check): a second reconcile_step must return Satisfied \
         (repair queue empty after phase 3's commit drained it)"
    );

    // Inode version must be UNCHANGED (still 2) — no duplicate commit.
    let inode_bytes_p4 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 4")
        .expect("inode must exist after phase 4");
    let inode_p4: InodeRecord =
        metadata::decode(&inode_bytes_p4).expect("decode inode after phase 4");
    assert_eq!(
        inode_p4.version, 2,
        "phase 4: inode version must still be 2 after Satisfied pass — \
         no duplicate commit must have landed"
    );

    // ====================================================================
    // Phase 5: read-after-commit
    //
    // Read fragments from the committed post-repair placement and collect which
    // servers hold intact fragments. Assert that EVERY server in the committed
    // placement is readable (ADR-0015 read-after-commit).
    // ====================================================================

    let committed_placement = &inode_p4.chunk_map[0].placement;
    let mut readable_servers: Vec<DServerId> = Vec::new();

    for (frag_index, &server_id) in committed_placement.iter().enumerate() {
        let frag_id = FragmentId {
            chunk: CHUNK,
            index: frag_index as u16,
        };
        let maybe_bytes = clients[server_id as usize]
            .get_fragment(frag_id)
            .await
            .unwrap_or_else(|e| {
                panic!("get_fragment index {frag_index} from server {server_id}: {e}")
            });
        if maybe_bytes
            .as_deref()
            .and_then(|b| {
                repair::intact_shard(
                    b,
                    frag_id,
                    EcScheme::ReedSolomon {
                        k: K as u8,
                        m: M as u8,
                    },
                )
            })
            .is_some()
        {
            readable_servers.push(server_id);
        }
    }

    // Every committed server must be readable (read-after-commit — ADR-0015):
    assert_read_after_commit(committed_placement, &readable_servers)
        .expect("read-after-commit invariant violated after repair");

    // ====================================================================
    // Phase 6: data integrity
    //
    // Read and reconstruct the original bytes from any K=6 intact shards of the
    // post-repair placement. Byte-identity proves the rebuilt fragment is correct.
    // ====================================================================

    let mut available: Vec<(usize, Vec<u8>)> = Vec::new();
    let mut missing: Vec<(usize, DServerId)> = Vec::new();

    for (frag_index, &server_id) in committed_placement.iter().enumerate() {
        let frag_id = FragmentId {
            chunk: CHUNK,
            index: frag_index as u16,
        };
        let maybe_bytes = clients[server_id as usize]
            .get_fragment(frag_id)
            .await
            .unwrap_or_else(|e| {
                panic!("get_fragment index {frag_index} from server {server_id}: {e}")
            });
        match maybe_bytes.as_deref().and_then(|b| {
            repair::intact_shard(
                b,
                frag_id,
                EcScheme::ReedSolomon {
                    k: K as u8,
                    m: M as u8,
                },
            )
        }) {
            Some(shard) => available.push((frag_index, shard)),
            None => missing.push((frag_index, server_id)),
        }
    }

    // Full redundancy: EVERY server named by the committed placement must hold an
    // intact fragment — including the freshly reconstructed one on the spare.
    assert!(
        missing.is_empty(),
        "every fragment in the post-repair placement must be present and intact, \
         including the rebuilt one on spare server {SPARE_INDEX}; \
         missing/corrupt (frag_index, server_id): {missing:?}"
    );
    assert_eq!(
        available.len(),
        N,
        "all N={N} placed fragments must be intact after reconstruction; got {}",
        available.len()
    );

    let reconstructed = erasure::reconstruct(K, M, data.len(), &available)
        .expect("erasure::reconstruct from post-repair shards");

    assert_eq!(
        reconstructed, data,
        "data reconstructed from the post-repair placement must be byte-identical \
         to the original (the rebuilt fragment on server {SPARE_INDEX} is correct)"
    );
}

// ---- Scenario test (live network partition) ----

/// **Tier-1 Jepsen LIVE network-partition leg** (issue #399, ADR-0039's named additive
/// upgrade).
///
/// The same six-phase consistency campaign as
/// [`jepsen_consistency_over_repair_under_partition_and_crash`], but Phase 2/3 isolate
/// server 1 with a **network-level packet drop** instead of a freezer-cgroup process
/// freeze — Jepsen's `:partition` nemesis, not `:pause`. The drop is an in-netns
/// `iptables -A INPUT ... -j DROP` on the gRPC port, injected by a throwaway sidecar that
/// shares the isolated container's network namespace ([`docker_netns_grpc_drop`]). Because
/// the container is **never disconnected, paused, or otherwise touched**, it stays in the
/// `running` state for the whole fault window — it keeps running on its own clock,
/// network-unreachable ([`assert_node_live_during_isolation`] asserts this, and the freeze
/// leg cannot exhibit it: `docker pause` reports `paused`, the gap ADR-0039 names) — AND
/// its host-published-port mapping survives, so Phase 3 heals by flushing the rule and the
/// scenario re-reaches the node at the SAME `127.0.0.1:<host-port>` endpoint it already
/// holds (the fix for the iteration-1 `docker network disconnect`/`connect` heal failure,
/// which tore down that mapping).
///
/// `#[ignore]`d for the same reason as the freeze leg: `cargo xtask ci`'s
/// `cargo test --workspace` compiles and type-checks this body (the born-at-tier compile
/// bar) without running it; the live execution happens only in the privileged
/// `WYRD_TIER1=1` `tier1-jepsen` job via `cargo xtask jepsen`, which now runs BOTH legs
/// in `xtask::faults::tier1_jepsen_isolation_legs` order (`xtask/src/faults.rs:179`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Tier-1: needs real containerized D servers — run via `cargo xtask jepsen`"]
async fn jepsen_consistency_over_repair_under_live_partition_and_crash() {
    // ---- Read cluster info from env (set by `cargo xtask jepsen`) ----

    let raw_endpoints = match std::env::var("WYRD_DSERVER_ENDPOINTS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier1_jepsen_consistency (live partition): WYRD_DSERVER_ENDPOINTS unset — \
                 skipping. Run `cargo xtask jepsen` to stand up the container D servers."
            );
            return;
        }
    };
    let victim_container = match std::env::var("WYRD_TIER1_VICTIM_CONTAINER") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier1_jepsen_consistency (live partition): WYRD_TIER1_VICTIM_CONTAINER \
                 unset — skipping. Run `cargo xtask jepsen` to supply the victim \
                 container name."
            );
            return;
        }
    };
    let partition_container = match std::env::var("WYRD_TIER1_PARTITION_CONTAINER") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier1_jepsen_consistency (live partition): WYRD_TIER1_PARTITION_CONTAINER \
                 unset — skipping. Run `cargo xtask jepsen` to supply the isolated \
                 container name."
            );
            return;
        }
    };
    // The D-server image (`wyrd-dserver:test`) reused as an in-netns `iptables` sidecar
    // for the partition. Unused by the freeze leg, so it is only required (not merely
    // read) here.
    let dserver_image = match std::env::var("WYRD_TIER1_DSERVER_IMAGE") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier1_jepsen_consistency (live partition): WYRD_TIER1_DSERVER_IMAGE \
                 unset — skipping. Run `cargo xtask jepsen` to supply the D-server \
                 image name (the iptables sidecar)."
            );
            return;
        }
    };

    let endpoints: Vec<String> = raw_endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    assert_eq!(
        endpoints.len(),
        JC_DSERVER_COUNT,
        "tier1_jepsen_consistency (live partition): need exactly {JC_DSERVER_COUNT} \
         endpoints, got {} ({raw_endpoints:?})",
        endpoints.len()
    );

    // ---- Connect to all 10 D servers ----

    let mut clients: Vec<GrpcChunkStore> = Vec::with_capacity(JC_DSERVER_COUNT);
    for endpoint in &endpoints {
        clients.push(connect(endpoint).await);
    }

    // ---- Setup: identical to the freeze leg (write RS(6,3) fragments 0-8, spare 9 empty) ----

    let data: Vec<u8> = (0u8..240).collect();
    let all_shards = erasure::encode(K, M, &data).expect("erasure::encode RS(6,3)");
    assert_eq!(all_shards.len(), N, "encoder must produce N={N} shards");

    for (index, shard) in all_shards.iter().enumerate() {
        let fragment_bytes = encode_ec_fragment(CHUNK, index as u16, K as u8, M as u8, shard);
        let frag_id = FragmentId {
            chunk: CHUNK,
            index: index as u16,
        };
        clients[index]
            .put_fragment(frag_id, fragment_bytes)
            .await
            .unwrap_or_else(|e| panic!("put_fragment index {index} to server {index}: {e}"));
    }

    let meta = CrashMeta::new();
    let chunk_ref = ChunkRef {
        id: CHUNK,
        scheme: EcScheme::ReedSolomon {
            k: K as u8,
            m: M as u8,
        },
        len: data.len() as u64,
        placement: (0..N as DServerId).collect(),
    };
    let inode_record = InodeRecord {
        size: data.len() as u64,
        chunk_map: vec![chunk_ref],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let create_outcome = metadata::create(&meta, 0, "test-file", INODE_ID, &inode_record)
        .await
        .expect("metadata::create inode");
    assert_eq!(
        create_outcome,
        CommitOutcome::Committed,
        "inode create must commit"
    );

    repair::enqueue_repair(&meta, CHUNK, "tier1-jepsen-live-partition-test")
        .await
        .expect("enqueue_repair");

    // ---- Kill server 0: crash fault (unchanged from the freeze leg) ----

    let kill_status = std::process::Command::new("docker")
        .arg("kill")
        .arg(&victim_container)
        .status()
        .expect("docker kill command");
    assert!(
        kill_status.success(),
        "`docker kill {victim_container}` failed (exit {kill_status}); \
         is the victim container name correct?"
    );

    let topology = healthy_topology(VICTIM_INDEX);
    let fleet: Vec<(DServerId, &dyn ChunkStore)> = (0..JC_DSERVER_COUNT)
        .filter(|&i| i != VICTIM_INDEX)
        .map(|i| (i as DServerId, &clients[i] as &dyn ChunkStore))
        .collect();

    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "tier1-jepsen-live-partition-zone")
        .await
        .expect("Custodian::elect");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());

    // ====================================================================
    // Phase 1: crash mid-repair (CrashMeta armed) — identical to the freeze leg.
    // ====================================================================

    meta.arm();
    let phase1_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await
    .expect("phase 1: reconcile_step (crash mid-repair)");

    assert_eq!(
        phase1_outcome,
        Reconciled::Satisfied,
        "phase 1 (crash mid-repair): reconcile_step must return Satisfied \
         when the version-conditional commit is intercepted by CrashMeta"
    );

    let frag0_id = FragmentId {
        chunk: CHUNK,
        index: 0,
    };
    let orphan_bytes = clients[SPARE_INDEX]
        .get_fragment(frag0_id)
        .await
        .expect("get_fragment fragment-0 from spare server after crash");

    let inode_bytes_p1 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 1")
        .expect("inode must exist after phase 1");
    let inode_p1: InodeRecord =
        metadata::decode(&inode_bytes_p1).expect("decode inode after phase 1");
    assert_eq!(
        inode_p1.version, 1,
        "committed inode must still be at version 1 after crash (no commit landed)"
    );
    assert_eq!(
        inode_p1.chunk_map[0].placement[VICTIM_INDEX], VICTIM_INDEX as DServerId,
        "committed placement[{VICTIM_INDEX}] must still reference the dead server \
         {VICTIM_INDEX} after crash — fully old, never a torn/hybrid chunk"
    );
    assert!(
        !inode_p1.chunk_map[0]
            .placement
            .contains(&(SPARE_INDEX as DServerId)),
        "spare server {SPARE_INDEX} must NOT appear in committed placement after crash; \
         the orphan is collectable garbage, not recorded corruption"
    );

    let committed_has_victim = inode_p1.chunk_map[0]
        .placement
        .contains(&(VICTIM_INDEX as DServerId));
    assert_commit_point_atomic(orphan_bytes.is_some(), committed_has_victim)
        .expect("commit-point-atomic invariant violated after crash mid-repair");

    // ====================================================================
    // Phase 2: LIVE network partition mid-repair (server 1 isolated by an in-netns
    // iptables DROP on its gRPC port — Jepsen's `:partition`, NOT `:pause`)
    //
    // Distinct from the freeze leg: server 1 is isolated by DROPping inbound gRPC packets
    // from a sidecar sharing its network namespace, never by suspending the container. The
    // node-liveness oracle proves the container stays `running` — its own clock keeps
    // running (the registration-lease renewal loop, `crates/server/src/cli.rs:309`, and
    // the request-timeout logic, `cli.rs:276`, both keep ticking) — for the whole fault
    // window, unlike `docker pause`, which would report `paused`. Because the container is
    // never disconnected, its published-port mapping is intact for Phase 3's heal.
    // ====================================================================

    meta.disarm();

    let drop_status = docker_netns_grpc_drop(&dserver_image, &partition_container, "-A");
    assert!(
        drop_status.success(),
        "injecting the in-netns iptables DROP on {partition_container} failed \
         (exit {drop_status}); is the container name / image correct and NET_ADMIN allowed?"
    );

    // Node-liveness-during-isolation: the isolated node must stay `running`, never
    // `paused` — the property that distinguishes this leg from the freeze leg
    // (issue #399's falsifiable new property; ADR-0039 says the repair-path OUTCOME is
    // unchanged over today's dumb D servers).
    let isolated_state = docker_container_state(&partition_container);
    assert_node_live_during_isolation(&isolated_state).unwrap_or_else(|e| {
        panic!("phase 2 (live network partition): {e} (container={partition_container})")
    });

    let phase2_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await;

    // Phase 2: a transient error from the network-partitioned server must abort the
    // pass — no partial commit (ADR-0015).
    assert!(
        phase2_outcome.is_err(),
        "phase 2 (live network partition mid-repair): reconcile_step must return Err \
         (transient fault from the network-partitioned server {PARTITION_INDEX} must \
         abort the pass — no partial commit); got: {phase2_outcome:?}"
    );

    let inode_bytes_p2 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 2")
        .expect("inode must exist after phase 2");
    let inode_p2: InodeRecord =
        metadata::decode(&inode_bytes_p2).expect("decode inode after phase 2");
    assert_eq!(
        inode_p2.version, 1,
        "phase 2: committed inode must still be at version 1 after partition abort; \
         no partial commit must have landed"
    );
    assert!(
        inode_p2.chunk_map[0]
            .placement
            .contains(&(VICTIM_INDEX as DServerId)),
        "phase 2: victim server {VICTIM_INDEX} must still be in committed placement \
         after partition abort — inode fully old"
    );

    // ====================================================================
    // Phase 3: heal the network partition and converge exactly once.
    //
    // Flush the DROP rule (heals the partition). Because the container was never
    // disconnected, its published-port mapping is intact — server 1 is reachable again at
    // the SAME endpoint the scenario already holds. With CrashMeta disarmed and server 1
    // reachable, reconcile_step succeeds: rebuilds fragment 0 from fragments 1-8 (K=6
    // survivors), places it on spare server 9, and commits.
    // ====================================================================

    let heal_status = docker_netns_grpc_drop(&dserver_image, &partition_container, "-D");
    assert!(
        heal_status.success(),
        "healing (removing) the in-netns iptables DROP on {partition_container} failed \
         (exit {heal_status})"
    );

    let phase3_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await
    .expect("phase 3: reconcile_step (after partition heal)");

    assert_eq!(
        phase3_outcome,
        Reconciled::Changed,
        "phase 3 (after partition heal): reconcile_step must return Changed \
         (repair committed)"
    );

    let inode_bytes_p3 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 3")
        .expect("inode must exist after phase 3");
    let inode_p3: InodeRecord =
        metadata::decode(&inode_bytes_p3).expect("decode inode after phase 3");

    assert_exactly_once_convergence(1, inode_p3.version)
        .expect("exactly-once convergence violated after phase 3");

    let new_placement = &inode_p3.chunk_map[0].placement;

    assert_redundancy_outcome(new_placement, VICTIM_INDEX as DServerId, N)
        .expect("redundancy outcome violated after reconstruction");

    assert_eq!(
        new_placement[VICTIM_INDEX], SPARE_INDEX as DServerId,
        "fragment {VICTIM_INDEX} must be re-placed on spare server {SPARE_INDEX} \
         (the only domain distinct from all survivor domains B–I)"
    );

    assert_distinct_domains(new_placement, |server_id| {
        DOMAINS.get(server_id as usize).copied()
    })
    .expect("distinct-domain invariant violated after reconstruction");

    // ====================================================================
    // Phase 4: exactly-once convergence check.
    // ====================================================================

    let phase4_outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&ReconstructionContext {
            meta: &meta,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
        }),
        None,
        0,
    )
    .await
    .expect("phase 4: reconcile_step (exactly-once check)");

    assert_eq!(
        phase4_outcome,
        Reconciled::Satisfied,
        "phase 4 (exactly-once check): a second reconcile_step must return Satisfied \
         (repair queue empty after phase 3's commit drained it)"
    );

    let inode_bytes_p4 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 4")
        .expect("inode must exist after phase 4");
    let inode_p4: InodeRecord =
        metadata::decode(&inode_bytes_p4).expect("decode inode after phase 4");
    assert_eq!(
        inode_p4.version, 2,
        "phase 4: inode version must still be 2 after Satisfied pass — \
         no duplicate commit must have landed"
    );

    // ====================================================================
    // Phase 5: read-after-commit.
    // ====================================================================

    let committed_placement = &inode_p4.chunk_map[0].placement;
    let mut readable_servers: Vec<DServerId> = Vec::new();

    for (frag_index, &server_id) in committed_placement.iter().enumerate() {
        let frag_id = FragmentId {
            chunk: CHUNK,
            index: frag_index as u16,
        };
        let maybe_bytes = clients[server_id as usize]
            .get_fragment(frag_id)
            .await
            .unwrap_or_else(|e| {
                panic!("get_fragment index {frag_index} from server {server_id}: {e}")
            });
        if maybe_bytes
            .as_deref()
            .and_then(|b| {
                repair::intact_shard(
                    b,
                    frag_id,
                    EcScheme::ReedSolomon {
                        k: K as u8,
                        m: M as u8,
                    },
                )
            })
            .is_some()
        {
            readable_servers.push(server_id);
        }
    }

    assert_read_after_commit(committed_placement, &readable_servers)
        .expect("read-after-commit invariant violated after repair");

    // ====================================================================
    // Phase 6: data integrity.
    // ====================================================================

    let mut available: Vec<(usize, Vec<u8>)> = Vec::new();
    let mut missing: Vec<(usize, DServerId)> = Vec::new();

    for (frag_index, &server_id) in committed_placement.iter().enumerate() {
        let frag_id = FragmentId {
            chunk: CHUNK,
            index: frag_index as u16,
        };
        let maybe_bytes = clients[server_id as usize]
            .get_fragment(frag_id)
            .await
            .unwrap_or_else(|e| {
                panic!("get_fragment index {frag_index} from server {server_id}: {e}")
            });
        match maybe_bytes.as_deref().and_then(|b| {
            repair::intact_shard(
                b,
                frag_id,
                EcScheme::ReedSolomon {
                    k: K as u8,
                    m: M as u8,
                },
            )
        }) {
            Some(shard) => available.push((frag_index, shard)),
            None => missing.push((frag_index, server_id)),
        }
    }

    assert!(
        missing.is_empty(),
        "every fragment in the post-repair placement must be present and intact, \
         including the rebuilt one on spare server {SPARE_INDEX}; \
         missing/corrupt (frag_index, server_id): {missing:?}"
    );
    assert_eq!(
        available.len(),
        N,
        "all N={N} placed fragments must be intact after reconstruction; got {}",
        available.len()
    );

    let reconstructed = erasure::reconstruct(K, M, data.len(), &available)
        .expect("erasure::reconstruct from post-repair shards");

    assert_eq!(
        reconstructed, data,
        "data reconstructed from the post-repair placement must be byte-identical \
         to the original (the rebuilt fragment on server {SPARE_INDEX} is correct)"
    );
}
