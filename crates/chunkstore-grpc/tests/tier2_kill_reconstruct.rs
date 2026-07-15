//! **Tier-2 kill-and-reconstruct scenario test** (Milestone 3.8, proposal 0005 §13.2,
//! `0005:409-411`; the `xtask` crate touch-point `0005:437-438`).
//!
//! What this test proves that no in-process test can: the **production custodian
//! reconstruction path** (`custodian::reconcile_step` → `reconstruction::reconcile`)
//! returns a D-server-kill event to full redundancy over **real gRPC D-server
//! containers** — real tonic transport, real prost (de)serialization, real connection
//! lifecycle. The three campaign properties it asserts (`0005:381-403`):
//!
//! 1. **Reconstruct-to-full-redundancy** — kill a D server; the reconstruction loop
//!    rebuilds the missing fragment(s) and repoints the placement record onto a healthy
//!    spare in a **distinct failure domain**, restoring all N fragments.
//! 2. **Commit-point-atomic repair under crash** — a crash before the
//!    version-conditional commit leaves the chunk **fully old, never a hybrid**
//!    (`0005:385-389`): the placed-but-uncommitted rebuilt fragment is **collectable
//!    garbage** on the spare server, the committed chunk map is untouched (victim still
//!    in placement — fully old inode).
//! 3. **Data integrity** — after successful reconstruction, erasure-decoding any k of
//!    the N post-repair fragments reconstructs the original bytes exactly.
//!
//! The RS(6,3) cluster has [`KR_DSERVER_COUNT`] = 10 servers: nine hold the initial
//! N=9 fragments; the tenth (the spare) is the re-placement target. Server 0 (domain A)
//! is killed; the spare (server 9, domain J) is the only available distinct domain after
//! the reconstruction selector excludes the survivor domains (B–I).
//!
//! **Gating**: the test body is `#[ignore]`d — `cargo xtask ci`'s `cargo test
//! --workspace` **compiles and type-checks** this file (proving the harness is real
//! API-bound Rust, not an env-var shell string) without running it. The live execution
//! (`--ignored`) happens only in the privileged off-Check job (`WYRD_TIER2=1`), run by
//! `cargo xtask kill-reconstruct`.
//!
//! **Born-at-tier coverage** at Check: the three assertion helpers
//! ([`assert_garbage_not_corruption`], [`assert_redundancy_outcome`],
//! [`assert_distinct_domains`]) are regular functions that the scenario test calls;
//! their own non-`#[ignore]`d unit tests run inside `cargo xtask ci`'s `cargo test
//! --workspace`. Stubbing or removing a helper causes both those unit tests and the
//! (compile-time) type-check of the scenario to fail — the born-at-tier seam is
//! load-bearing.

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
/// Number of D-server containers for the kill-reconstruct cluster:
/// N servers for the initial placement + 1 spare for the re-placed rebuilt fragment.
pub(crate) const KR_DSERVER_COUNT: usize = N + 1;
/// 0-indexed server that is killed (server 0, failure domain A).
const VICTIM_INDEX: usize = 0;
/// 0-indexed spare server that receives the rebuilt fragment (server 9, failure domain J).
const SPARE_INDEX: usize = N; // = 9
/// The inode id used in the test.
const INODE_ID: InodeId = 1;
/// The chunk id used in the test — a recognisable sentinel value.
const CHUNK: ChunkId = 0x0002_DEAD_CAFE_BEEF;
/// One distinct failure domain per D server. Domains A–I cover servers 0–8 (the
/// initial placement); domain J covers server 9 (the spare). The reconstruction
/// selector excludes survivor domains B–I and picks domain J → server 9.
const DOMAINS: [&str; KR_DSERVER_COUNT] = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];

// ---- In-memory MetadataStore ----
//
// The same shape used by the Tier-0 DST campaign (`crates/dst/tests/custodian.rs:76-113`).
// The reconstruction loop runs over the `MetadataStore` seam, so this in-memory backend
// drives the same production `reconcile_step` path as the DST — what Tier-2 adds is
// running it against a REAL gRPC fleet.

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
// Mirrors `CrashMeta` in `crates/dst/tests/custodian.rs:124-167`. While *armed*, it
// drops the reconstruction loop's version-conditional repoint commit (the single batch
// that carries a positive precondition), modelling the custodian dying just before its
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

// ---- Assertion helpers ----
//
// These are regular functions (not `#[cfg(test)]`-only) so the scenario test body calls
// them and the unit tests below are load-bearing: removing or stubbing a helper causes
// the `#[test]` unit tests AND the compile-time type-check of the scenario to fail.

/// Assert the garbage-not-corruption invariant after a crash mid-repair
/// (`0005:277`, `0005:385-389`):
///
/// - The placed-but-uncommitted rebuilt fragment **EXISTS** on the new server
///   (`orphaned_fragment_exists`): the write-before-commit ordering in
///   `reconstruction::repair_chunk` ensures a crash leaves a collectable orphan, not
///   missing bytes.
/// - The committed chunk map **DOES** reference the victim
///   (`committed_placement_has_victim == true`): the version-conditional commit never
///   landed, so the inode is at its prior (fully-old) value — victim still in
///   placement. If the victim is absent from the committed placement after a crash,
///   the commit partially landed, leaving a torn/hybrid chunk — a violation.
///
/// Returns `Ok(())` when both hold, `Err` describing the first violation otherwise.
pub(crate) fn assert_garbage_not_corruption(
    orphaned_fragment_exists: bool,
    committed_placement_has_victim: bool,
) -> AssertResult {
    if !orphaned_fragment_exists {
        return Err(
            "placed-but-uncommitted rebuilt fragment is absent after a crash mid-repair; \
             reconstruction writes the fragment BEFORE its commit, so a crash must leave it \
             as collectable garbage — its absence means the write-before-commit ordering \
             was violated"
                .to_string(),
        );
    }
    // After a crash before the version-conditional commit, the inode is FULLY OLD:
    // the victim IS still in the committed placement — the commit to remove it never
    // landed. If the victim is NOT in the committed placement, the commit partially
    // applied and the chunk is a torn/hybrid — which violates the invariant.
    if !committed_placement_has_victim {
        return Err(
            "the committed chunk map does NOT reference the victim after a crash mid-repair; \
             the version-conditional commit must NOT have landed — the inode must be fully old \
             (victim still in committed placement), never a torn/hybrid chunk"
                .to_string(),
        );
    }
    Ok(())
}

/// Assert the post-reconstruction redundancy outcome:
/// - exactly `n` fragments are placed (full redundancy restored);
/// - the killed server (`victim_id`) is no longer in the placement.
///
/// Returns `Ok(())` when both hold, `Err` describing the first violation otherwise.
pub(crate) fn assert_redundancy_outcome(
    placement: &[DServerId],
    victim_id: DServerId,
    n: usize,
) -> AssertResult {
    if placement.len() != n {
        return Err(format!(
            "expected {n} fragments in placement after reconstruction, got {}; \
             full redundancy not restored",
            placement.len()
        ));
    }
    if placement.contains(&victim_id) {
        return Err(format!(
            "killed server {victim_id} still appears in the post-reconstruction placement; \
             reconstruction must not re-place onto the dead server"
        ));
    }
    Ok(())
}

/// Assert that all fragments in `placement` are on **distinct** failure domains,
/// using `domain_of(server_id)` to look up each server's domain label.
///
/// The distinct-domain invariant (`0005:241-242`, `0005:382-383`): each fragment of a
/// replicated chunk must reside on a separate failure domain so a single-domain loss
/// (e.g. a rack failure) cannot take more than `m` fragments. Reconstruction preserves
/// this by selecting re-placement servers via `select_distinct_domains_excluding`.
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

// ---- Helper unit tests (non-#[ignore], run at Check) ----
//
// These are the born-at-tier coverage for the assertion helpers. They run inside
// `cargo xtask ci`'s `cargo test --workspace` without requiring a container runtime.
// If a helper is stubbed or removed, these tests fail.

#[test]
fn garbage_not_corruption_passes_when_orphan_exists_and_victim_still_in_committed_placement() {
    // After a crash before the version-conditional commit: the orphan exists on the
    // spare server AND the victim IS still in the committed placement (fully old inode).
    assert!(
        assert_garbage_not_corruption(true, true).is_ok(),
        "orphan present, victim still in committed placement: invariant must hold"
    );
}

#[test]
fn garbage_not_corruption_fails_when_orphan_is_absent() {
    // No orphan on the spare server: write-before-commit ordering was violated.
    let result = assert_garbage_not_corruption(false, true);
    assert!(result.is_err(), "absent orphan must fail the invariant");
    let err = result.unwrap_err();
    assert!(
        err.contains("absent"),
        "error must name the violation: {err}"
    );
}

#[test]
fn garbage_not_corruption_fails_when_victim_not_in_committed_placement() {
    // Victim absent from committed placement after a crash: the commit partially landed
    // → torn/hybrid chunk → violation of the fully-old guarantee.
    let result = assert_garbage_not_corruption(true, false);
    assert!(
        result.is_err(),
        "victim absent from committed placement after crash must fail the invariant"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("hybrid"),
        "error must name the torn/hybrid violation: {err}"
    );
}

#[test]
fn redundancy_outcome_passes_when_victim_absent_and_n_fragments() {
    // Victim = 0, N = 3. Placement [1, 2, 3]: victim absent, exactly 3 fragments.
    assert!(
        assert_redundancy_outcome(&[1, 2, 3], 0, 3).is_ok(),
        "victim 0 absent, 3 fragments: must pass"
    );
}

#[test]
fn redundancy_outcome_fails_when_victim_in_placement() {
    let result = assert_redundancy_outcome(&[0, 1, 2], 0, 3);
    assert!(result.is_err(), "victim 0 in placement must fail");
    let err = result.unwrap_err();
    assert!(
        err.contains("still appears"),
        "error must name the violation: {err}"
    );
}

#[test]
fn redundancy_outcome_fails_when_wrong_fragment_count() {
    // Only 2 fragments in placement, need 3.
    let result = assert_redundancy_outcome(&[1, 2], 0, 3);
    assert!(result.is_err(), "too few fragments must fail");
    let err = result.unwrap_err();
    assert!(
        err.contains('2'),
        "error must cite the observed count: {err}"
    );
}

#[test]
fn distinct_domains_passes_for_all_different() {
    let domain_of =
        |id: DServerId| -> Option<&'static str> { ["A", "B", "C", "D"].get(id as usize).copied() };
    // [1, 2, 3] → domains B, C, D — all distinct.
    assert!(
        assert_distinct_domains(&[1, 2, 3], domain_of).is_ok(),
        "all distinct domains: must pass"
    );
}

#[test]
fn distinct_domains_fails_for_duplicate_domain() {
    // Servers 1 and 3 share domain "B" in this test topology.
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

// ---- Scenario helpers ----

/// Dial a D server, retrying briefly so a just-launched container that has not yet
/// finished binding its listener is waited on rather than failing the test.
/// Mirrors `connect` in `crates/chunkstore-grpc/tests/tier2_integration.rs:42-54`.
async fn connect(endpoint: &str) -> GrpcChunkStore {
    let mut last_err = None;
    for _ in 0..50 {
        match GrpcChunkStore::connect(endpoint.to_string()).await {
            Ok(c) => return c,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("could not connect to D server `{endpoint}`: {last_err:?}");
}

/// Build the failure-domain topology for the kill-reconstruct cluster,
/// **excluding** the dead `victim` server so the selector never picks it
/// for re-placement.
fn healthy_topology(victim: usize) -> Topology {
    let mut t = Topology::new(vec![]);
    for (i, &domain) in DOMAINS.iter().enumerate() {
        if i != victim {
            t.register(i as DServerId, domain);
        }
    }
    t
}

// ---- Scenario test ----

/// **Tier-2 kill-and-reconstruct campaign** (M3.8, `0005:409-411`).
///
/// The `#[ignore]` attribute keeps this body out of `cargo xtask ci` (unprivileged,
/// container-free, ADR-0016); the `#[tokio::test]` attribute ensures `cargo xtask ci`'s
/// `cargo test --workspace` COMPILES and TYPE-CHECKS the body (the "born-at-tier" bar:
/// an API regression that stubbed out `reconcile_step` would fail to compile). The body
/// runs only in the privileged `WYRD_TIER2=1` job via `cargo xtask kill-reconstruct`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Tier-2: needs real containerized D servers — run via `cargo xtask kill-reconstruct`"]
async fn kill_reconstruct_restores_full_redundancy_in_distinct_domains() {
    let raw_endpoints = match std::env::var("WYRD_DSERVER_ENDPOINTS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier2_kill_reconstruct: WYRD_DSERVER_ENDPOINTS unset — skipping. \
                 Run `cargo xtask kill-reconstruct` to stand up the container D servers."
            );
            return;
        }
    };
    let victim_container = match std::env::var("WYRD_TIER2_VICTIM_CONTAINER") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier2_kill_reconstruct: WYRD_TIER2_VICTIM_CONTAINER unset — skipping. \
                 Run `cargo xtask kill-reconstruct` to supply the victim container name."
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
        KR_DSERVER_COUNT,
        "tier2_kill_reconstruct: need exactly {KR_DSERVER_COUNT} endpoints, got {} ({raw_endpoints:?})",
        endpoints.len()
    );

    // --- Connect to all 10 D servers ---

    let mut clients: Vec<GrpcChunkStore> = Vec::with_capacity(KR_DSERVER_COUNT);
    for endpoint in &endpoints {
        clients.push(connect(endpoint).await);
    }

    // --- Setup: write RS(6,3) chunk fragments to servers 0–8; spare server 9 holds nothing ---

    // 240 bytes → 40-byte shards under RS(6,3): enough for non-trivial reconstruction.
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

    // Create the inode: committed, placement = [0, 1, 2, …, 8].
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
    };
    let create_outcome = metadata::create(&meta, 0, "test-file", INODE_ID, &inode_record)
        .await
        .expect("metadata::create inode");
    assert_eq!(
        create_outcome,
        CommitOutcome::Committed,
        "inode create must commit"
    );

    // Enqueue a repair obligation for CHUNK, as a health-check producer (scrub or read path)
    // would after detecting that server 0 is no longer serving its fragment.
    repair::enqueue_repair(&meta, CHUNK, "tier2-test")
        .await
        .expect("enqueue_repair");

    // --- Kill server 0 via docker kill ---

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

    // --- Reconstruction context: exclude dead server 0 from fleet and topology ---
    //
    // Fleet: servers 1–9 (alive). Server 0 is not in the fleet; the reconstruction
    // code sees `stores.get(&0) == None` → fragment 0 is classified as missing and
    // rebuilt from the K=6 surviving fragments on servers 1–8.
    //
    // Topology (excluding victim): servers 1–9, domains B–J. The selector excludes
    // survivor domains B–I and finds only domain J → picks server 9 as the re-placement.

    let topology = healthy_topology(VICTIM_INDEX);
    let fleet: Vec<(DServerId, &dyn ChunkStore)> = (0..KR_DSERVER_COUNT)
        .filter(|&i| i != VICTIM_INDEX)
        .map(|i| (i as DServerId, &clients[i] as &dyn ChunkStore))
        .collect();

    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "tier2-zone")
        .await
        .expect("Custodian::elect");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());

    // ====================================================================
    // Phase 1: crash mid-repair (CrashMeta armed)
    //
    // The reconstruction loop writes the rebuilt fragment to server 9 (a real gRPC
    // put_fragment over the wire) THEN attempts the version-conditional commit on the
    // in-memory metadata store. CrashMeta intercepts the commit (returns Conflict),
    // simulating the custodian crashing just before it lands — exactly `0005:385-386`.
    //
    // Expected: Reconciled::Satisfied (the commit was lost; nothing changed in meta).
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
    .expect("phase 1: reconcile_step");

    assert_eq!(
        phase1_outcome,
        Reconciled::Satisfied,
        "phase 1 (crash mid-repair): reconcile_step must return Satisfied \
         when the version-conditional commit is intercepted by CrashMeta"
    );

    // Garbage-not-corruption check — property 2 (`0005:385-389`):

    // (a) The placed-but-uncommitted rebuilt fragment EXISTS on spare server 9
    //     (reconstruction writes fragment BEFORE the commit, `0005:277`):
    let frag0_id = FragmentId {
        chunk: CHUNK,
        index: 0,
    };
    let orphan_bytes = clients[SPARE_INDEX]
        .get_fragment(frag0_id)
        .await
        .expect("get_fragment fragment-0 from spare server after crash");

    // (b) The committed inode is FULLY OLD — version unchanged (1), victim still at
    //     placement[0], spare NOT in the committed placement:
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
        "committed placement[{VICTIM_INDEX}] must still reference the dead server {VICTIM_INDEX} \
         after crash — fully old, never a torn/hybrid chunk (`0005:385-386`)"
    );
    assert!(
        !inode_p1.chunk_map[0]
            .placement
            .contains(&(SPARE_INDEX as DServerId)),
        "spare server {SPARE_INDEX} must NOT appear in the committed placement after crash; \
         the placed-but-uncommitted fragment is collectable garbage, not recorded corruption"
    );

    // Summary assertion via the born-at-tier helper: orphan exists, victim still in
    // committed placement (fully old inode — the commit never landed).
    let committed_has_victim = inode_p1.chunk_map[0]
        .placement
        .contains(&(VICTIM_INDEX as DServerId));
    assert_garbage_not_corruption(orphan_bytes.is_some(), committed_has_victim)
        .expect("garbage-not-corruption invariant violated after crash mid-repair");

    // ====================================================================
    // Phase 2: real repair (CrashMeta disarmed, repair obligation still queued)
    //
    // The disarmed custodian resumes where it left off: the repair queue still holds
    // CHUNK (the dropped commit left it un-drained). On this pass the version-
    // conditional commit lands: placement[0] moves from victim (0) to spare (9),
    // version bumps to 2.
    //
    // Expected: Reconciled::Changed.
    // ====================================================================

    meta.disarm();
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
    .await
    .expect("phase 2: reconcile_step");

    assert_eq!(
        phase2_outcome,
        Reconciled::Changed,
        "phase 2 (real repair): reconcile_step must return Changed after the commit lands"
    );

    // Verify the committed inode: full redundancy restored in distinct domains.
    let inode_bytes_p2 = meta
        .get(&metadata::inode_key(INODE_ID))
        .await
        .expect("meta.get inode after phase 2")
        .expect("inode must exist after phase 2");
    let inode_p2: InodeRecord =
        metadata::decode(&inode_bytes_p2).expect("decode inode after phase 2");

    assert_eq!(
        inode_p2.version, 2,
        "committed inode must be at version 2 after successful reconstruction \
         (exactly one version-conditional commit landed)"
    );

    let new_placement = &inode_p2.chunk_map[0].placement;

    // Redundancy outcome: N fragments, victim absent.
    assert_redundancy_outcome(new_placement, VICTIM_INDEX as DServerId, N)
        .expect("redundancy outcome violated after reconstruction");

    // Specific placement check: victim slot re-filled with the spare server.
    assert_eq!(
        new_placement[VICTIM_INDEX], SPARE_INDEX as DServerId,
        "fragment {VICTIM_INDEX} must be re-placed on spare server {SPARE_INDEX} \
         (the only domain distinct from all survivor domains B–I)"
    );

    // Distinct-domain invariant (`0005:241-242`, `0005:382-383`): all N servers in
    // the new placement must be on distinct failure domains.
    assert_distinct_domains(new_placement, |server_id| {
        DOMAINS.get(server_id as usize).copied()
    })
    .expect("distinct-domain invariant violated after reconstruction");

    // ====================================================================
    // Phase 3: data integrity
    //
    // Read fragments from the post-repair placement ([9, 1, 2, …, 8]) and
    // reconstruct the original bytes from any K=6 intact shards. Byte-identity
    // proves the rebuilt fragment is correct (not a random overwrite or truncation).
    // ====================================================================

    let mut available: Vec<(usize, Vec<u8>)> = Vec::new();
    let mut missing: Vec<(usize, DServerId)> = Vec::new();
    for (frag_index, &server_id) in new_placement.iter().enumerate() {
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
        match maybe_bytes.as_deref().and_then(|bytes| {
            repair::intact_shard(
                bytes,
                frag_id,
                EcScheme::ReedSolomon {
                    k: K as u8,
                    m: M as u8,
                },
            )
        }) {
            Some(shard) => available.push((frag_index, shard)),
            // A placed fragment that is absent (`Ok(None)`) or fails its integrity
            // check is a repair FAILURE, not something to skip over.
            None => missing.push((frag_index, server_id)),
        }
    }

    // Full redundancy means EVERY server named by the committed placement actually
    // holds an intact fragment — including the freshly reconstructed one on the
    // spare. A weaker `available.len() >= K` check would pass on the K=6 surviving
    // old shards alone even if reconstruction never wrote the spare's fragment, so
    // it cannot prove redundancy was restored (Codex review, #271). Require all N.
    assert!(
        missing.is_empty(),
        "every fragment in the post-repair placement must be present and intact, \
         including the rebuilt one on spare server {SPARE_INDEX}; missing/corrupt \
         (frag_index, server_id): {missing:?}"
    );
    assert_eq!(
        available.len(),
        N,
        "all N={N} placed fragments must be intact after reconstruction (full \
         redundancy restored, not merely >= K survivors); got {}",
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
