//! Deterministic-simulation testing (DST) seams and harness skeleton.
//!
//! DST is the spine of the project's correctness story from Milestone 0
//! (ADR-0009): production logic is written against the abstract time and disk
//! seams in this crate, then driven by a single-threaded, seed-reproducible
//! simulator in which every bug reproduces from its seed. **madsim** is the
//! intended production runtime (it simulates time, scheduling, network, and
//! randomness); these seams are shaped to be driven by it, and a madsim-backed
//! [`Sim`] runner is wired in as the first async protocol code lands.
//!
//! This crate is a real dependency, not a helper, so the determinism story
//! cannot rot as the system grows. At M0 it provides the trait seams, a
//! seed-derived deterministic RNG, fault-injection hook points, and a runner
//! skeleton.

#![forbid(unsafe_code)]

use rand::{Rng, RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Abstract logical time. Production code reads time through this seam instead
/// of the wall clock, so the simulator controls time and a run is reproducible.
pub trait Clock {
    /// The current logical time, in milliseconds since the simulation epoch.
    fn now_millis(&self) -> u64;
}

/// The production [`Clock`]: real wall-clock time, in milliseconds since the Unix
/// epoch. Used by single-process backends outside a simulation.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A manually-advanced [`Clock`] for deterministic tests: cheap to clone and
/// share (the handle and the code under test see the same time), and advanced
/// explicitly so expiry and timeout logic is exercised without real waiting.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    millis: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl ManualClock {
    /// A clock started at `start_millis`.
    pub fn new(start_millis: u64) -> Self {
        Self {
            millis: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(start_millis)),
        }
    }

    /// Advance the clock by `millis`.
    pub fn advance(&self, millis: u64) {
        self.millis
            .fetch_add(millis, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the clock to an absolute `millis`.
    pub fn set(&self, millis: u64) {
        self.millis
            .store(millis, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.millis.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Abstract durable storage. Production code performs disk I/O through this seam
/// so the simulator can model latency, reordering, and faults deterministically.
pub trait Disk {
    /// Read the bytes previously written under `key`, if any.
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, DiskError>;

    /// Write `bytes` under `key`. Not durable until [`Disk::sync`] succeeds.
    fn write(&mut self, key: &str, bytes: &[u8]) -> Result<(), DiskError>;

    /// Flush previously written bytes to durable storage.
    fn sync(&mut self) -> Result<(), DiskError>;
}

/// A disk fault surfaced by the simulator (or, later, a real backend).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskError {
    /// The operation failed due to an injected or real I/O fault.
    Io(String),
}

impl std::fmt::Display for DiskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskError::Io(msg) => write!(f, "disk i/o fault: {msg}"),
        }
    }
}

impl std::error::Error for DiskError {}

/// The operations at which a fault may be injected. Extended as more seams gain
/// fault coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// A disk read.
    DiskRead,
    /// A disk write.
    DiskWrite,
    /// A durability sync.
    DiskSync,
    /// A fragment `put` travelling to a D server (the write fan-out).
    FragmentPut,
    /// A fragment `get` travelling from a D server (the any-`k` read).
    FragmentFetch,
}

/// Decides whether to inject a fault at a given point. The default
/// implementation injects nothing; a campaign supplies one that fails
/// operations according to the seed.
pub trait FaultInjector {
    /// Return `true` to inject a fault at `point`.
    fn should_fail(&mut self, point: FaultPoint) -> bool;
}

/// A fault injector that never injects a fault — the baseline for a run that
/// exercises only the happy path.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoFaults;

impl FaultInjector for NoFaults {
    fn should_fail(&mut self, _point: FaultPoint) -> bool {
        false
    }
}

/// The **network seam** (proposal 0004, "DST and integration tests": a network
/// abstraction alongside the [`Clock`]/[`Disk`] seams). A `NetFault` is a fault
/// the simulator injects on the link between the client and one D server, so the
/// *real* gRPC `ChunkStore` wire code can be exercised under seed-reproducible
/// drops, delays, partitions, and corruption (ADR-0009, Tier-1 properties 2–4).
///
/// The fault *model* lives here, free of any transport dependency, so it is
/// import-light; a campaign maps each variant onto madsim's network controls
/// (clog the link for `Drop`/`Partition`/`Delay`) or a corrupting store wrapper
/// (`Corrupt`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetFault {
    /// Outbound traffic to the D server is dropped — the fragment put or fetch
    /// never arrives. Modelled by clogging the client→server link.
    Drop,
    /// Traffic to the D server is delayed by this many milliseconds before
    /// delivery — a slow fragment the any-`k` read should not wait on.
    Delay(u64),
    /// The link is cut in both directions — the D server is partitioned away for
    /// the duration of the operation.
    Partition,
    /// The bytes the D server returns are corrupted, so the fragment fails its
    /// client-side checksum and is treated as **absent** (re-read elsewhere).
    Corrupt,
}

/// Decides whether — and how — to fault the network for the D server at a given
/// index. Mirrors [`FaultInjector`] for the network seam; the default injects
/// nothing.
pub trait NetFaultInjector {
    /// The fault to inject for the D server at `store_index` on `point`, or
    /// `None` to let the operation through untouched.
    fn fault_for(&mut self, store_index: usize, point: FaultPoint) -> Option<NetFault>;
}

/// A network-fault plan fixed once from the run seed: a chosen set of D-server
/// links, each with the fault to apply. Drawing the selection from the
/// simulation RNG is what makes the whole campaign reproduce from its seed
/// (ADR-0009) — a bug-finding seed replays the *same* faulted links.
#[derive(Debug, Clone, Default)]
pub struct SeededNetFaults {
    faults: std::collections::BTreeMap<usize, NetFault>,
}

impl SeededNetFaults {
    /// A plan from an explicit map of `store_index → fault` — e.g. corrupt the
    /// fragment on D server 2.
    pub fn new(faults: std::collections::BTreeMap<usize, NetFault>) -> Self {
        Self { faults }
    }

    /// Pick up to `max` **distinct** D-server indices in `0..n`, each to be hit
    /// with `fault`, drawing the choice from `rng`. Picking fewer than or equal
    /// to `max` (never more) keeps a `k`-of-`n` read above its `k` survivors.
    pub fn pick<R: Rng>(rng: &mut R, n: usize, max: usize, fault: NetFault) -> Self {
        let mut indices: Vec<usize> = (0..n).collect();
        // Fisher–Yates prefix shuffle: draw `max` distinct indices deterministically.
        let take = max.min(n);
        for i in 0..take {
            let j = i + (rng.next_u32() as usize) % (n - i);
            indices.swap(i, j);
        }
        let faults = indices
            .into_iter()
            .take(take)
            .map(|index| (index, fault))
            .collect();
        Self { faults }
    }

    /// The chosen faults, by D-server index.
    pub fn faults(&self) -> &std::collections::BTreeMap<usize, NetFault> {
        &self.faults
    }

    /// Whether the D server at `index` carries a fault in this plan.
    pub fn is_faulted(&self, index: usize) -> bool {
        self.faults.contains_key(&index)
    }
}

impl NetFaultInjector for SeededNetFaults {
    fn fault_for(&mut self, store_index: usize, _point: FaultPoint) -> Option<NetFault> {
        self.faults.get(&store_index).copied()
    }
}

/// The **storage seam** fault model (proposal 0005 §"DST and tests": "a **bit-rot /
/// fragment-loss fault seam** and a **D-server-kill seam** alongside the existing
/// `Clock`/`Disk`/`Network` seams", `0005:434-435`). A `StorageFault` is a fault the
/// custodian campaign injects on a D server's **stored fragment bytes**, so the four
/// custodian loops (scrub, reconstruction, GC) can be driven against seed-reproducible
/// bit rot and fragment loss — the storage-plane sibling of [`NetFault`] (which faults
/// the *link*, not the stored byte).
///
/// The fault *model* lives here, free of any chunk-store or chunk-format dependency, so
/// it stays import-light; a campaign maps each variant onto a concrete `ChunkStore`
/// operation (drop the stored bytes for `Lost`; flip a payload byte for `BitRot`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFault {
    /// The fragment's bytes are **gone** — the D server no longer holds them: a disk
    /// failure, a deleted shard, or (the **D-server-kill** seam) every fragment a
    /// killed D server held. A loss the custodian reads around and reconstructs
    /// (Q1, `0005:381-384`).
    Lost,
    /// The fragment's bytes are **silently corrupted in place** (bit rot): still
    /// present, but they fail their self-describing checksum, so scrub must exclude
    /// the shard (never decode it) and enqueue the chunk for repair (Q2,
    /// `0005:390-393`).
    BitRot,
}

/// Decides whether — and how — to fault the **stored bytes** of the D server at a given
/// index. Mirrors [`NetFaultInjector`] for the storage seam; the default injects
/// nothing.
pub trait StorageFaultInjector {
    /// The fault to inject for the D server at `store_index`, or `None` to leave its
    /// stored bytes untouched.
    fn storage_fault_for(&mut self, store_index: usize) -> Option<StorageFault>;
}

/// A storage-fault plan fixed once from the run seed: a chosen set of D servers, each
/// with the fault to apply to its stored fragment bytes. The storage-seam sibling of
/// [`SeededNetFaults`]; drawing the selection from the simulation RNG is what makes the
/// custodian campaign reproduce from its seed (ADR-0009) — a bug-finding seed replays
/// the *same* killed / rotted servers as a permanent regression.
#[derive(Debug, Clone, Default)]
pub struct SeededStorageFaults {
    faults: std::collections::BTreeMap<usize, StorageFault>,
}

impl SeededStorageFaults {
    /// A plan from an explicit map of `store_index → fault` — e.g. rot the fragment on
    /// D server 2.
    pub fn new(faults: std::collections::BTreeMap<usize, StorageFault>) -> Self {
        Self { faults }
    }

    /// Pick up to `max` **distinct** D-server indices in `0..n`, each to be hit with
    /// `fault`, drawing the choice from `rng`. Picking no more than `max` keeps an
    /// erasure-coded chunk above its `k` survivors so the loop can still reconstruct
    /// (the same bound [`SeededNetFaults::pick`] keeps for the link seam).
    pub fn pick<R: Rng>(rng: &mut R, n: usize, max: usize, fault: StorageFault) -> Self {
        let mut indices: Vec<usize> = (0..n).collect();
        // Fisher–Yates prefix shuffle: draw `max` distinct indices deterministically.
        let take = max.min(n);
        for i in 0..take {
            let j = i + (rng.next_u32() as usize) % (n - i);
            indices.swap(i, j);
        }
        let faults = indices
            .into_iter()
            .take(take)
            .map(|index| (index, fault))
            .collect();
        Self { faults }
    }

    /// The **D-server-kill seam**: pick exactly **one** D server in `0..n` to kill,
    /// drawing the victim from `rng`. A killed server's fragment is [`StorageFault::Lost`]
    /// — the Q1 "kill a D server" injection (`0005:381-384`), reproduced from the seed.
    pub fn kill<R: Rng>(rng: &mut R, n: usize) -> Self {
        Self::pick(rng, n, 1, StorageFault::Lost)
    }

    /// The chosen faults, by D-server index.
    pub fn faults(&self) -> &std::collections::BTreeMap<usize, StorageFault> {
        &self.faults
    }

    /// Whether the D server at `index` carries a fault in this plan.
    pub fn is_faulted(&self, index: usize) -> bool {
        self.faults.contains_key(&index)
    }
}

impl StorageFaultInjector for SeededStorageFaults {
    fn storage_fault_for(&mut self, store_index: usize) -> Option<StorageFault> {
        self.faults.get(&store_index).copied()
    }
}

/// The **metadata-backend fault seam** (M4.6, #257; proposal 0015 §"DST and tests",
/// PR-sequence item 6; ADR-0015 single-zone consistency contract). Where [`NetFault`]
/// faults a client→D-server *link* and [`StorageFault`] faults a *stored fragment byte*,
/// the metadata swap (redb → a real ≥3-replica TiKV Raft group behind the unchanged
/// [`wyrd_traits::MetadataStore`] trait) is faulted by **partitioning voters of the Raft
/// group**. TiKV/PD run their own Raft consensus, so the load-bearing question the Tier-1
/// consistency leg rests on is *quorum*: which side of a **symmetric** partition retains a
/// strict majority and therefore stays writable.
///
/// This is **pure arithmetic** (no cluster, no container, no transport dependency), so the
/// ≥3-replica reachability reasoning is unit-testable inside the unprivileged `cargo xtask
/// ci` gate — the same "born-at-tier flippable coverage" bar the sibling seams meet. It
/// makes concrete the invariant that a *minority* partition against a linearizable Raft
/// store **cannot** cause split-brain or a lost update (proposal 0015; ADR-0015): the
/// majority side keeps quorum, the isolated minority goes read-only, and there is no second
/// writer to diverge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionOutcome {
    /// The unisolated (majority) side keeps a strict quorum and stays writable; the
    /// isolated minority is read-only until the heal.
    MajorityWritable,
    /// The **isolated** side holds a strict majority (more than half were isolated), so it
    /// is the writable side instead — the mirror image of [`Self::MajorityWritable`].
    IsolatedWritable,
    /// An **even split**: neither side holds a strict majority, so no side can commit
    /// (Raft stalls rather than diverge). Only reachable for an even `total`.
    NoQuorum,
    /// The "partition" isolates **nobody or everybody** — a no-op that never materialized
    /// (the Invariant-B fault-effect precondition: a partition is evidence only if it
    /// actually splits the group).
    NotMaterialized,
}

/// The strict-majority (quorum) size for a Raft group of `total` voters: `⌊total/2⌋ + 1`.
/// A side can commit only if it holds at least this many voters.
#[must_use]
pub fn quorum(total: usize) -> usize {
    total / 2 + 1
}

/// Decide the [`PartitionOutcome`] for a **symmetric** partition of a `total`-voter Raft
/// group that isolates `isolated` voters. Pure — the whole point is that the ≥3-replica
/// quorum reasoning is checkable without standing up a cluster.
///
/// A partition that isolates nobody (`isolated == 0`) or everybody (`isolated == total`)
/// is [`PartitionOutcome::NotMaterialized`]. Otherwise exactly one of three things holds:
/// the majority side is writable, the isolated side is writable (if it holds the majority),
/// or neither is (an even split).
#[must_use]
pub fn partition_outcome(total: usize, isolated: usize) -> PartitionOutcome {
    if total == 0 || isolated == 0 || isolated >= total {
        return PartitionOutcome::NotMaterialized;
    }
    let remaining = total - isolated;
    let q = quorum(total);
    if remaining >= q {
        PartitionOutcome::MajorityWritable
    } else if isolated >= q {
        PartitionOutcome::IsolatedWritable
    } else {
        PartitionOutcome::NoQuorum
    }
}

/// Whether a symmetric partition of `isolated`/`total` voters **materialized** — i.e. it
/// actually split the group (isolated at least one voter but not all). The Invariant-B
/// precondition for treating a partition leg as evidence: a partition that isolates nobody
/// or everybody is a no-op and proves nothing (the v6 asymmetric inbound-only-DROP defect,
/// which left the target reachable, is exactly a non-materialized partition).
#[must_use]
pub fn partition_materialized(total: usize, isolated: usize) -> bool {
    total > 0 && isolated > 0 && isolated < total
}

/// **Exactly-once convergence** (ADR-0015 commit-point atomicity): the metadata inode's
/// version advanced by **exactly one** across a commit-and-heal — not zero (no commit
/// slipped through) and not two-or-more (a double-commit). An independent arithmetic
/// oracle over the observed before/after versions, kept SEPARATE from the read-after-commit
/// signal so a diagnostic shows precisely which ADR-0015 clause failed (the v6 defect
/// collapsed both into one `scenario.is_ok()` bit).
#[must_use]
pub fn converged_exactly_once(version_before: u64, version_after: u64) -> bool {
    version_after == version_before.wrapping_add(1)
}

/// The **two INDEPENDENT ADR-0015 single-zone signals** the Tier-1 metadata
/// consistency-over-the-swap leg carries, plus the Invariant-B fault-effect gate. Kept as
/// separate fields (not one collapsed boolean — the v6 defect) so the verdict names the
/// exact clause that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsistencySignals {
    /// **Read-after-commit** (ADR-0015): the value committed before the fault is still
    /// readable — byte-for-byte — after the partition heals. A committed value that
    /// becomes unreadable is a violation, not a valid state.
    pub read_after_commit: bool,
    /// **Exactly-once convergence** (ADR-0015 commit-point atomicity): the inode version
    /// advanced by exactly one across the heal (see [`converged_exactly_once`]).
    pub converged_once: bool,
    /// **Fault materialized** (Invariant B): the injected partition provably took effect —
    /// observed from the PEER/PD side as a **lost heartbeat** (not by probing the dropped port,
    /// and not via PD's slow-to-flip administrative `state_name`), AND healed completely (see
    /// [`partition_materialized`], [`partition_took_effect`], [`heartbeat_is_fresh`], and
    /// [`heal_is_complete`]). A leg whose fault was a no-op is NOT evidence.
    pub fault_materialized: bool,
    /// **No lost update under write-write contention** (ADR-0015 commit-point re-check; the
    /// iteration-12 teeth). The `get_for_update` commit-point re-check only produces a
    /// `Conflict` under CONCURRENT contention on the same key, so a strictly sequential
    /// single-writer leg can never observe its regression (the iteration-12 adversary
    /// refutation: deleting the re-check left every assertion green). This signal is computed
    /// by [`no_lost_update`] over the contended-CAS outcome and the stale-CAS probe.
    pub no_lost_update: bool,
}

/// The Tier-1 consistency leg passes only when **all four** signals hold: read-after-commit
/// AND exactly-once convergence AND the fault materialized AND no lost update under
/// contention. Each is an independent input, so a regression flips exactly the failing clause.
#[must_use]
pub fn consistency_passes(s: &ConsistencySignals) -> bool {
    s.read_after_commit && s.converged_once && s.fault_materialized && s.no_lost_update
}

/// **No-lost-update verdict for the contended commit point** (ADR-0015; M4.6 #257, the
/// iteration-12 teeth). With ≥2 concurrent writers racing the SAME compare-and-swap on the
/// version key across the fault window, exactly **one** may report `Committed`
/// (`committed_contenders == 1`: zero means a commit was lost outright, two-or-more means a
/// stale precondition was admitted — a lost update), and a deliberately **stale** CAS probe
/// (`require` on a version the cell has already left) must be **rejected**
/// (`stale_probe_committed == false`). A missing/mis-ordered `get_for_update` commit-point
/// re-check (`crates/metadata-tikv/src/lib.rs:555-573`) flips one or both inputs, so this
/// oracle — pure arithmetic, unit-checked at Check, consumed by the live leg — is the signal a
/// real commit-point regression turns red.
#[must_use]
pub fn no_lost_update(committed_contenders: usize, stale_probe_committed: bool) -> bool {
    committed_contenders == 1 && !stale_probe_committed
}

/// Did the injected **symmetric** partition actually isolate the target metadata node,
/// **observed from the peer/PD side**? (M4.6, #257; Invariant B.)
///
/// `connected_before` / `connected_during` are what **PD** (the Raft peers' coordinator)
/// reports about the target store — `true` when PD still counts the store's **heartbeat
/// fresh** (see [`heartbeat_is_fresh`]), `false` once PD has stopped receiving heartbeats from
/// it. This is deliberately **not** a probe of the dropped port from the test host (the v6/v7
/// defect: that only proves the rule blocks *the probe*, not that the node is isolated from its
/// peers), and deliberately **not** PD's administrative `state_name` (the iter-11 defect:
/// `state_name` stays `"Up"` through a short partition and only flips after
/// `max-store-down-time`, ~30min, so a seconds-long scenario window can never observe it
/// change). A partition is evidence only if the peer's own transient-liveness view flipped:
/// heartbeat fresh before, stale during. A one-way or probe-only cut leaves the heartbeat fresh
/// (`connected_during == true`) and fails this oracle — exactly the no-op shape Invariant B
/// forbids.
#[must_use]
pub fn partition_took_effect(connected_before: bool, connected_during: bool) -> bool {
    connected_before && !connected_during
}

/// Parse the target store's `last_heartbeat_ts` (PD's RFC3339 timestamp of its last heartbeat)
/// from the raw `/pd/api/v1/stores` JSON body, as **nanoseconds since the Unix epoch**, for the
/// store whose `address` contains `target_ip`. `None` when that store or the field is absent or
/// unparsable. (M4.6, #257; the iter-11 fault-effect fix.)
///
/// This is the **transient-liveness** field the metadata partition oracle keys off — **not**
/// PD's `state_name`, which is the ADMINISTRATIVE state and stays `"Up"` through a short
/// partition (it only flips to `Down` after `max-store-down-time`, default ~30min), so a
/// seconds-long scenario window could never observe it change (the iter-11 defect that made the
/// live leg unpassable). A partitioned voter stops heartbeating PD, so `last_heartbeat_ts` stops
/// advancing and its age grows past a few store-heartbeat intervals (default 10s) within the
/// window. The **field selection** is unit-checkable inside the unprivileged `cargo xtask ci`
/// gate (the live scenario calls this very function, so a regression here flips both the unit
/// test and the live leg).
///
/// PD emits the heartbeat as `"last_heartbeat_ts":"<rfc3339>"` — an RFC3339 timestamp **string**
/// in the `status` object (e.g. `"2019-03-21T14:14:22.961171958+08:00"`), **not** a bare
/// `last_heartbeat` integer (the field that shape assumed does not exist in a real PD response;
/// Codex #453). Within a store entry `"address":"<ip>:<port>"` (in the `store` object) precedes
/// it, so the first `last_heartbeat_ts` after the target's address is that store's own. It is
/// converted to epoch nanoseconds via `chrono` so the downstream age threshold
/// ([`heartbeat_is_fresh`]) is unchanged.
#[must_use]
pub fn parse_store_last_heartbeat(stores_json: &str, target_ip: &str) -> Option<i128> {
    let compact: String = stores_json.chars().filter(|c| !c.is_whitespace()).collect();
    let needle = format!("\"address\":\"{target_ip}:");
    let at = compact.find(&needle)?;
    let rest = &compact[at..];
    let key = "\"last_heartbeat_ts\":\"";
    let ks = rest.find(key)? + key.len();
    let after = &rest[ks..];
    // `last_heartbeat_ts` is a quoted RFC3339 string; take up to the closing quote and
    // convert to nanoseconds since the Unix epoch (parse-only — no clock read).
    let end = after.find('"')?;
    chrono::DateTime::parse_from_rfc3339(&after[..end])
        .ok()?
        .timestamp_nanos_opt()
        .map(i128::from)
}

/// Parse the **leader's `store_id`** for the first region in a raw PD `/pd/api/v1/regions`
/// JSON body, or `None` when no leader is recorded. (M4.6, #257; the iteration-12
/// leader-isolation fix.)
///
/// Why the LEADER: the iteration-12 adversary showed that symmetrically cutting a **minority
/// follower** of a linearizable Raft group can never change a commit outcome — the majority
/// side keeps quorum and the leader keeps serving — so that cut is exactly the "hollow flip"
/// the brief forbids. Isolating the **region leader** forces a leader election and perturbs
/// the in-flight commit path for real. Scope note: a fresh ≥3-replica test cluster
/// (`deploy/tikv-multi-replica`) starts with a handful of system regions whose leaders all
/// sit on ONE store (PD's balancer hasn't spread them within the scenario window — verified
/// empirically on pd v8.5.1: five regions, one leader store), so "first region's leader" IS
/// the leader of the region under test; a long-lived multi-region cluster would need
/// region-by-key resolution (documented limitation, not this leg's shape).
///
/// Pure string parse — no HTTP, no dependency — so the live leg's leader **selection** is
/// unit-checkable inside the unprivileged gate; the live scenario calls this very function.
#[must_use]
pub fn parse_first_region_leader_store_id(regions_json: &str) -> Option<u64> {
    let compact: String = regions_json
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let at = compact.find("\"leader\":")?;
    let rest = &compact[at..];
    let key = "\"store_id\":";
    let ks = rest.find(key)? + key.len();
    let after = &rest[ks..];
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    after[..end].parse::<u64>().ok()
}

/// Parse the **IP** (address minus the `:port`) of the store with id `store_id` from a raw PD
/// `/pd/api/v1/stores` JSON body, or `None` when that store is absent. (M4.6, #257; the
/// iteration-12 leader-isolation fix — maps [`parse_first_region_leader_store_id`]'s answer to
/// the loopback IP the symmetric partition drops.) Pure string parse, unit-checked at Check,
/// consumed by the live leg.
#[must_use]
pub fn parse_store_ip(stores_json: &str, store_id: u64) -> Option<String> {
    let compact: String = stores_json.chars().filter(|c| !c.is_whitespace()).collect();
    let needle = format!("\"id\":{store_id},");
    let at = compact.find(&needle)?;
    let rest = &compact[at..];
    let key = "\"address\":\"";
    let ks = rest.find(key)? + key.len();
    let after = &rest[ks..];
    let end = after.find('"')?;
    after[..end].rsplit_once(':').map(|(ip, _)| ip.to_string())
}

/// Resolve which container owns `target_ip` from a `ip=container,ip=container` map (the
/// runner-exported `WYRD_TIER1_NETNS_MAP`), or `None` when the IP is unmapped. (M4.6, #257;
/// the iteration-13 netns-cut fix.)
///
/// Why a netns map: the symmetric partition is applied **inside the target node's own
/// network namespace** (`docker run --network container:<name> … iptables …`) — under the
/// old host-networking topology every node's outbound traffic was sourced from
/// `127.0.0.1`, so a host-side per-IP cut was a provable no-op (the fault-effect oracle
/// caught it live). The scenario resolves the leader's IP first, then this map names the
/// netns to cut. Pure string parse, unit-checked at Check, consumed by the live leg — a
/// mapping regression flips both.
#[must_use]
pub fn parse_netns_map(map: &str, target_ip: &str) -> Option<String> {
    map.split(',').find_map(|pair| {
        let (ip, container) = pair.trim().split_once('=')?;
        (ip.trim() == target_ip && !container.trim().is_empty())
            .then(|| container.trim().to_string())
    })
}

/// Whether PD still counts the target store **live** — its last heartbeat is fresher than
/// `max_staleness`. `age = now_nanos - last_heartbeat_nanos` (clamped at zero for clock skew),
/// both nanoseconds since the Unix epoch. This is the boolean fed to [`partition_took_effect`]:
/// fresh before the cut, **stale** during it. (M4.6, #257; the iter-11 fault-effect fix.)
///
/// A store that never heartbeated (`last_heartbeat <= 0`) or one long-silent is stale → not
/// live. Deciding liveness on **heartbeat age** rather than PD's `state_name` is what lets the
/// fault-effect oracle observe a real partition within a seconds-long window. Pure arithmetic,
/// so it is unit-checkable at Check; the live scenario calls it, so a threshold regression
/// flips both.
#[must_use]
pub fn heartbeat_is_fresh(
    last_heartbeat_nanos: i128,
    now_nanos: i128,
    max_staleness: std::time::Duration,
) -> bool {
    if last_heartbeat_nanos <= 0 {
        return false;
    }
    let age_nanos = (now_nanos - last_heartbeat_nanos).max(0);
    (age_nanos as u128) < max_staleness.as_nanos()
}

/// Did the partition also **heal** completely? (M4.6, #257; Invariant B / the v6 review.)
///
/// The live partition must heal **every** isolation rule it applied (the v6 leg dropped two
/// ports but healed only one), and PD must report the target store `Up` again
/// (`connected_after_heal`) or host firewall state leaked. `applied` and `healed` are the
/// isolation-rule identifiers the runner recorded; the heal is complete only when every
/// applied rule was removed AND the peer view recovered.
#[must_use]
pub fn heal_is_complete(applied: &[String], healed: &[String], connected_after_heal: bool) -> bool {
    connected_after_heal && applied.iter().all(|r| healed.contains(r))
}

/// A seed-reproducible simulation context.
///
/// Everything non-deterministic a component needs — randomness, time, fault
/// decisions — is drawn from here, so a whole run is a pure function of its
/// seed. The runner is single-threaded by construction.
pub struct Sim {
    seed: u64,
    rng: ChaCha8Rng,
    clock_millis: u64,
}

impl Sim {
    /// Create a simulation from a seed. The same seed always produces the same
    /// run.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            rng: ChaCha8Rng::seed_from_u64(seed),
            clock_millis: 0,
        }
    }

    /// The seed this simulation was created from — record it to reproduce a run.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The deterministic RNG. All randomness in a run must be drawn from here.
    pub fn rng(&mut self) -> &mut impl Rng {
        &mut self.rng
    }

    /// Draw a uniformly random value of type `T` from the deterministic RNG.
    pub fn gen<T>(&mut self) -> T
    where
        rand::distr::StandardUniform: rand::distr::Distribution<T>,
    {
        self.rng.random()
    }

    /// Advance logical time by `millis`.
    pub fn advance(&mut self, millis: u64) {
        self.clock_millis += millis;
    }
}

impl Clock for Sim {
    fn now_millis(&self) -> u64 {
        self.clock_millis
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_reproduces_the_same_run() {
        // The core DST promise: a run is a pure function of its seed.
        let mut a = Sim::new(42);
        let mut b = Sim::new(42);
        let xs: Vec<u64> = (0..16).map(|_| a.gen()).collect();
        let ys: Vec<u64> = (0..16).map(|_| b.gen()).collect();
        assert_eq!(xs, ys);
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Sim::new(1);
        let mut b = Sim::new(2);
        let xs: Vec<u64> = (0..16).map(|_| a.gen()).collect();
        let ys: Vec<u64> = (0..16).map(|_| b.gen()).collect();
        assert_ne!(xs, ys);
    }

    #[test]
    fn clock_advances_monotonically() {
        let mut sim = Sim::new(0);
        assert_eq!(sim.now_millis(), 0);
        sim.advance(10);
        sim.advance(5);
        assert_eq!(sim.now_millis(), 15);
    }

    #[test]
    fn no_faults_injects_nothing() {
        let mut faults = NoFaults;
        assert!(!faults.should_fail(FaultPoint::DiskWrite));
    }

    #[test]
    fn seeded_net_faults_pick_is_reproducible_and_bounded() {
        // The network-seam promise: the faulted-link selection is a pure
        // function of the seed, and never exceeds `max` (so a k-of-n read keeps
        // its k survivors).
        let mut a = ChaCha8Rng::seed_from_u64(7);
        let mut b = ChaCha8Rng::seed_from_u64(7);
        let pa = SeededNetFaults::pick(&mut a, 9, 3, NetFault::Drop);
        let pb = SeededNetFaults::pick(&mut b, 9, 3, NetFault::Drop);
        assert_eq!(pa.faults(), pb.faults(), "same seed → same faulted links");
        assert!(pa.faults().len() <= 3, "never more than `max` faults");
        assert!(
            pa.faults().keys().all(|&i| i < 9),
            "faulted indices are valid D servers"
        );
    }

    #[test]
    fn seeded_net_faults_reports_per_store() {
        let mut faults = SeededNetFaults::new([(2, NetFault::Corrupt)].into_iter().collect());
        assert!(faults.is_faulted(2));
        assert!(!faults.is_faulted(0));
        assert_eq!(
            faults.fault_for(2, FaultPoint::FragmentFetch),
            Some(NetFault::Corrupt)
        );
        assert_eq!(faults.fault_for(0, FaultPoint::FragmentFetch), None);
    }

    #[test]
    fn seeded_storage_faults_pick_is_reproducible_and_bounded() {
        // The storage-seam promise: the rotted/lost-server selection is a pure function
        // of the seed, and never exceeds `max` (so an erasure-coded chunk keeps its `k`
        // survivors for the loop to reconstruct from).
        let mut a = ChaCha8Rng::seed_from_u64(11);
        let mut b = ChaCha8Rng::seed_from_u64(11);
        let pa = SeededStorageFaults::pick(&mut a, 9, 3, StorageFault::BitRot);
        let pb = SeededStorageFaults::pick(&mut b, 9, 3, StorageFault::BitRot);
        assert_eq!(pa.faults(), pb.faults(), "same seed → same rotted shards");
        assert!(pa.faults().len() <= 3, "never more than `max` faults");
        assert!(
            pa.faults().keys().all(|&i| i < 9),
            "faulted indices are valid D servers"
        );
    }

    #[test]
    fn seeded_storage_faults_kill_picks_exactly_one_lost_server() {
        // The D-server-kill seam: exactly one server is killed, and its fragment is Lost
        // — reproducible from the seed.
        let mut a = ChaCha8Rng::seed_from_u64(99);
        let mut b = ChaCha8Rng::seed_from_u64(99);
        let ka = SeededStorageFaults::kill(&mut a, 9);
        let kb = SeededStorageFaults::kill(&mut b, 9);
        assert_eq!(ka.faults(), kb.faults(), "same seed → same killed server");
        assert_eq!(ka.faults().len(), 1, "kill drops exactly one D server");
        let (&victim, &fault) = ka.faults().iter().next().unwrap();
        assert!(victim < 9, "the killed server is a valid D server");
        assert_eq!(
            fault,
            StorageFault::Lost,
            "a killed server's fragment is lost"
        );
    }

    #[test]
    fn seeded_storage_faults_reports_per_store() {
        let mut faults =
            SeededStorageFaults::new([(2, StorageFault::BitRot)].into_iter().collect());
        assert!(faults.is_faulted(2));
        assert!(!faults.is_faulted(0));
        assert_eq!(faults.storage_fault_for(2), Some(StorageFault::BitRot));
        assert_eq!(faults.storage_fault_for(0), None);
    }

    // ── Metadata-backend fault seam (M4.6, #257) ──────────────────────────────────
    //
    // Independent oracles: each assertion uses a HAND-COMPUTED expectation (a quorum
    // table), NOT the literal the function returns, so a mutation of the arithmetic
    // (e.g. an off-by-one `total/2` instead of `total/2 + 1`) flips these RED — the
    // non-tautological bar (the iter-1 defect the Success criterion forbids).

    #[test]
    fn quorum_is_strict_majority() {
        // ⌊total/2⌋ + 1, computed independently of the function under test.
        assert_eq!(quorum(1), 1);
        assert_eq!(quorum(3), 2);
        assert_eq!(quorum(4), 3);
        assert_eq!(quorum(5), 3);
        assert_eq!(quorum(9), 5);
    }

    #[test]
    fn minority_partition_leaves_the_majority_writable() {
        // The load-bearing ≥3-replica case (ADR-0015 single-zone): isolate ONE voter of
        // three — the majority side (2) keeps quorum and stays writable; the isolated
        // minority (1 < 2) goes read-only. There is no second writer, so no divergence:
        // this is exactly why "exactly-one-winner goes red" can NEVER be the binding flip
        // against a linearizable store (the Invariant the Success criterion pins).
        assert_eq!(partition_outcome(3, 1), PartitionOutcome::MajorityWritable);
        assert_eq!(partition_outcome(5, 2), PartitionOutcome::MajorityWritable);
    }

    #[test]
    fn majority_partition_makes_the_isolated_side_writable() {
        // The mirror image: isolate MORE than half. The isolated side holds the quorum,
        // so it is the writable side (the unisolated remnant goes read-only).
        assert_eq!(partition_outcome(3, 2), PartitionOutcome::IsolatedWritable);
        assert_eq!(partition_outcome(5, 3), PartitionOutcome::IsolatedWritable);
    }

    #[test]
    fn even_split_stalls_rather_than_diverges() {
        // An even split hands neither side a strict majority — Raft stalls, it never lets
        // two sides both commit (the property that makes split-brain unreachable).
        assert_eq!(partition_outcome(4, 2), PartitionOutcome::NoQuorum);
        assert_eq!(partition_outcome(6, 3), PartitionOutcome::NoQuorum);
    }

    #[test]
    fn a_no_op_partition_is_not_materialized() {
        // Isolating nobody or everybody is a no-op: the Invariant-B fault-effect
        // precondition is FALSE, so such a leg is not evidence (the v6 asymmetric
        // inbound-only DROP left the target reachable — a non-materialized partition).
        assert_eq!(partition_outcome(3, 0), PartitionOutcome::NotMaterialized);
        assert_eq!(partition_outcome(3, 3), PartitionOutcome::NotMaterialized);
        assert!(!partition_materialized(3, 0));
        assert!(!partition_materialized(3, 3));
        assert!(!partition_materialized(0, 0));
        // A real split of at least one voter (but not all) DID materialize.
        assert!(partition_materialized(3, 1));
        assert!(partition_materialized(3, 2));
    }

    #[test]
    fn convergence_is_exactly_one_version_step() {
        // Independent oracle: version must advance by EXACTLY one across the heal.
        assert!(converged_exactly_once(7, 8), "advanced by one → converged");
        assert!(!converged_exactly_once(7, 7), "no commit → NOT converged");
        assert!(
            !converged_exactly_once(7, 9),
            "double-commit → NOT converged"
        );
        assert!(
            !converged_exactly_once(7, 6),
            "went backwards → NOT converged"
        );
    }

    #[test]
    fn consistency_needs_all_four_independent_signals() {
        let ok = ConsistencySignals {
            read_after_commit: true,
            converged_once: true,
            fault_materialized: true,
            no_lost_update: true,
        };
        assert!(consistency_passes(&ok), "all four signals hold → PASS");
        // Each clause is INDEPENDENTLY load-bearing: negating any one fails the verdict,
        // proving the four are not collapsed into one bit (the v6 defect).
        assert!(!consistency_passes(&ConsistencySignals {
            read_after_commit: false,
            ..ok
        }));
        assert!(!consistency_passes(&ConsistencySignals {
            converged_once: false,
            ..ok
        }));
        assert!(
            !consistency_passes(&ConsistencySignals {
                fault_materialized: false,
                ..ok
            }),
            "a leg whose fault did NOT materialize is not evidence, even if the data \
             assertions passed (Invariant B)"
        );
        assert!(
            !consistency_passes(&ConsistencySignals {
                no_lost_update: false,
                ..ok
            }),
            "a leg that admitted a lost update under contention fails, even if the \
             single-writer data assertions passed (the iteration-12 teeth)"
        );
    }

    #[test]
    fn lost_updates_are_caught_by_the_contention_arithmetic() {
        // Exactly one CAS winner and a rejected stale probe — the only healthy outcome.
        assert!(no_lost_update(1, false));
        // Two contenders both reported Committed on the SAME compare-and-swap: a stale
        // precondition was admitted — the lost update the get_for_update re-check exists to
        // prevent. This is the input a deleted re-check produces (iteration-12 adversary).
        assert!(!no_lost_update(2, false), "double-commit = lost update");
        // Nobody committed: the update was lost outright (or the leg never contended).
        assert!(!no_lost_update(0, false), "zero winners = lost commit");
        // The stale probe (require on a version the cell already left) was admitted: a
        // blind write slipped the commit point, independent of the contender count.
        assert!(!no_lost_update(1, true), "admitted stale CAS = lost update");
    }

    #[test]
    fn region_leader_and_store_ip_resolve_from_pd_bodies() {
        // Hand-computed expectations over canned PD payload shapes (not the literal a
        // function returns). The leader lives in the region's `leader` object; peers also
        // carry store_ids the parse must NOT confuse with the leader's.
        let regions = r#"{"count":1,"regions":[
            {"id":4,"start_key":"","end_key":"",
             "peers":[{"id":5,"store_id":1},{"id":6,"store_id":2},{"id":7,"store_id":3}],
             "leader":{"id":6,"store_id":2},
             "written_bytes":0}
        ]}"#;
        assert_eq!(parse_first_region_leader_store_id(regions), Some(2));
        // No leader recorded (election in flight) → honest None, not a spurious store.
        let leaderless = r#"{"count":1,"regions":[{"id":4,"peers":[{"id":5,"store_id":1}]}]}"#;
        assert_eq!(parse_first_region_leader_store_id(leaderless), None);

        // Store-id → IP over the same /stores shape the heartbeat oracle parses.
        let stores = r#"{"count":2,"stores":[
            {"store":{"id":1,"address":"127.0.0.1:20160","state_name":"Up"},
             "status":{"last_heartbeat_ts":"1970-01-01T00:00:00.000000111Z"}},
            {"store":{"id":2,"address":"127.0.0.2:20161","state_name":"Up"},
             "status":{"last_heartbeat_ts":"1970-01-01T00:00:00.000000222Z"}}
        ]}"#;
        assert_eq!(parse_store_ip(stores, 2).as_deref(), Some("127.0.0.2"));
        assert_eq!(parse_store_ip(stores, 1).as_deref(), Some("127.0.0.1"));
        // A store PD never listed → None (the leg must then refuse to cut anything).
        assert_eq!(parse_store_ip(stores, 9), None);
    }

    #[test]
    fn netns_map_resolves_the_target_container_only() {
        let map = "172.30.57.11=wyrd-tier1-metadata-tikv-0-1, \
                   172.30.57.12=wyrd-tier1-metadata-tikv-1-1,\
                   172.30.57.13=wyrd-tier1-metadata-tikv-2-1";
        assert_eq!(
            parse_netns_map(map, "172.30.57.12").as_deref(),
            Some("wyrd-tier1-metadata-tikv-1-1")
        );
        assert_eq!(
            parse_netns_map(map, "172.30.57.11").as_deref(),
            Some("wyrd-tier1-metadata-tikv-0-1")
        );
        // An unmapped IP (e.g. PD's own) must resolve to nothing — the leg then refuses to
        // cut anything rather than cutting a guess.
        assert_eq!(parse_netns_map(map, "172.30.57.10"), None);
        // Malformed entries are skipped, not misread.
        assert_eq!(parse_netns_map("no-equals-here,=x,a=", "a"), None);
    }

    #[test]
    fn partition_is_evidence_only_when_peers_lose_the_target() {
        // The ONLY combination that materialized, observed from PD's side: the peers saw
        // the store Up before, and lost it (Disconnected) during. This is a peer-side view,
        // not a probe of the dropped port from the test host.
        assert!(
            partition_took_effect(true, false),
            "PD saw the store before, lost it during → the partition materialized"
        );
        // The v6/v7 one-way / probe-only shape: PD still sees the store connected during the
        // "partition" → it was a no-op, NOT evidence (red when the fault is a no-op).
        assert!(
            !partition_took_effect(true, true),
            "PD still sees the store during → the partition was a no-op (Invariant B)"
        );
        // A store PD never saw up before proves nothing either.
        assert!(!partition_took_effect(false, false));
        assert!(!partition_took_effect(false, true));
    }

    #[test]
    fn last_heartbeat_is_parsed_for_the_addressed_store() {
        // Two stores; the oracle must pick the TARGET's own `last_heartbeat_ts` (the RFC3339
        // heartbeat string in its `status` object), not a neighbour's — independent
        // hand-computed expectation (the timestamps below are 111 / 222 ns past the epoch), not
        // the literal the function returns. A regression that read `state_name` (the iter-11
        // defect), the wrong store's field, or the non-existent bare `last_heartbeat` integer
        // (Codex #453) flips this red.
        let body = r#"{"count":2,"stores":[
            {"store":{"id":1,"address":"127.0.0.1:20160","state_name":"Up"},
             "status":{"capacity":"1GiB","last_heartbeat_ts":"1970-01-01T00:00:00.000000111Z"}},
            {"store":{"id":2,"address":"127.0.0.2:20160","state_name":"Up"},
             "status":{"capacity":"1GiB","last_heartbeat_ts":"1970-01-01T00:00:00.000000222Z"}}
        ]}"#;
        assert_eq!(parse_store_last_heartbeat(body, "127.0.0.2"), Some(222));
        assert_eq!(parse_store_last_heartbeat(body, "127.0.0.1"), Some(111));
        // A store PD never listed → no heartbeat to read (honest None, not a spurious value).
        assert_eq!(parse_store_last_heartbeat(body, "127.0.0.9"), None);

        // A real PD timestamp — non-UTC offset, full-nanosecond precision (the exact shape from
        // the PD API docs). It must parse to a positive epoch-nanos value, not None.
        let real = r#"{"count":1,"stores":[
            {"store":{"id":1,"address":"10.0.0.5:20160","state_name":"Up"},
             "status":{"last_heartbeat_ts":"2019-03-21T14:14:22.961171958+08:00"}}
        ]}"#;
        assert!(matches!(parse_store_last_heartbeat(real, "10.0.0.5"), Some(n) if n > 0));

        // The shape this oracle used to assume — a bare `last_heartbeat` integer — is NOT what
        // PD emits, and must now read as absent rather than silently succeeding (Codex #453).
        let stale_shape = r#"{"count":1,"stores":[
            {"store":{"id":1,"address":"10.0.0.6:20160","state_name":"Up"},
             "status":{"last_heartbeat":123456789}}
        ]}"#;
        assert_eq!(parse_store_last_heartbeat(stale_shape, "10.0.0.6"), None);
    }

    #[test]
    fn heartbeat_freshness_is_an_age_threshold_not_state_name() {
        // Independent oracle: liveness is `now - last_heartbeat < max_staleness`, computed by
        // hand here. This is the transient signal that flips within seconds — the iter-11 fix
        // (state_name would stay "Up" and never flip in-window).
        let sec = 1_000_000_000_i128; // one second in nanoseconds
        let now = 100 * sec;
        let window = std::time::Duration::from_secs(20);
        // Fresh: 5s old, threshold 20s → PD still sees it live.
        assert!(heartbeat_is_fresh(now - 5 * sec, now, window));
        // Stale: 30s old, threshold 20s → PD lost it (a real partition the oracle must catch).
        assert!(!heartbeat_is_fresh(now - 30 * sec, now, window));
        // Exactly at the threshold is NOT fresh (strict `<`).
        assert!(!heartbeat_is_fresh(now - 20 * sec, now, window));
        // Never heartbeated (0) → not live.
        assert!(!heartbeat_is_fresh(0, now, window));
        // Clock skew (heartbeat "in the future") clamps to age 0 → fresh, never spuriously stale.
        assert!(heartbeat_is_fresh(now + 5 * sec, now, window));
    }

    #[test]
    fn heal_must_undo_every_isolation_rule_and_restore_the_peer_view() {
        let both = vec!["in-127.0.0.2".to_string(), "out-127.0.0.2".to_string()];
        // The complete heal: every applied rule removed AND PD sees the store Up again.
        assert!(
            heal_is_complete(&both, &both, true),
            "every applied rule healed + peer view restored → complete heal"
        );
        // The v6 leak: applied two rules but healed only one — leaks host firewall state.
        assert!(
            !heal_is_complete(&both, &both[..1], true),
            "an un-removed isolation rule leaks host state — NOT a complete heal"
        );
        // Every rule removed but PD still doesn't see the store → the heal did not take.
        assert!(
            !heal_is_complete(&both[..1], &both[..1], false),
            "peer still down after heal → the heal did not restore the target"
        );
    }
}
