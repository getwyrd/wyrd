//! **The deployed custodian role actually RUNS garbage collection — reclaiming the bytes a
//! delete orphaned once the reader-safe grace window elapses, while preserving the evidence
//! of any garbage it could not safely sweep this pass.** (#554.)
//!
//! GC is the ONLY thing that reclaims fragment bytes: the write path deliberately
//! marks-not-deletes (`metadata::unlink` writes an orphan grace record for every fragment the
//! removed object placed, `crates/core/src/metadata.rs:369-415`) so a reader inside the grace
//! window is never torn mid-read; the custodian GC (`crates/custodian/src/gc.rs`) reclaims each
//! recorded orphan later, once its grace deadline has passed. But on `origin/main` the deployed
//! run loop `run_reconstruction_until` passes `None` where the `GcContext` goes for BOTH of its
//! `reconcile_pass` calls, so the deployed role never runs GC and every delete/overwrite leaks
//! its displaced bytes into the ledger forever (`crates/server/src/custodian.rs`).
//!
//! Every test drives the SAME production wiring `wyrd custodian` runs
//! (`cli::cmd_custodian` → `CustodianService::run_reconstruction_until` →
//! `live_reconstruction_view` + `reconcile_pass`) over in-memory metadata + trait-store fleets
//! with a logical clock — no Docker, no live cluster, exactly as `custodian_day_one.rs` does. It
//! pins the MECHANISM the fix ships, not reader-safety:
//!
//! * [`deployed_role_reclaims_orphaned_bytes_after_grace_elapses`] — put → delete → advance the
//!   clock past the grace window → drive the deployed loop: the orphaned fragments are physically
//!   removed from the D-servers, while a still-live object loses nothing. RED on base (the run
//!   loop never runs GC, so the bytes remain forever); GREEN with the fix.
//! * [`deployed_role_keeps_orphaned_bytes_within_the_grace_window`] — the reader-safe half: with
//!   the clock still INSIDE the grace window the same loop reclaims nothing (no premature
//!   collection). Green on base and with the fix — it pins the no-reclaim-before guarantee.
//! * [`armed_deployed_role_reclaims_expired_pending_lease_garbage`] — GC's SECOND input: the
//!   bytes a crashed write fan-out left under an expired `pending:` lease are reclaimed through
//!   the deployed loop (brief: "expired pending leases are likewise GC's input") — but ONLY under
//!   the operator's `--gc-expired-pending` attestation, because "expired" cannot be trusted
//!   while any producer stamps logical-clock leases.
//! * [`deployed_role_defers_expired_pending_garbage_by_default`] — the #557 GATE: without
//!   `--gc-expired-pending` the deployed role defers every `pending:` entry however expired it
//!   looks, so a CLI lease stamped at logical zero (`cli.rs` `NOW_MILLIS = 0`) can never have its
//!   still-in-flight fan-out swept by a wall-clocked pass. Fails if the deployed default ever
//!   regresses to reclaiming.
//! * [`deployed_role_defers_gc_and_preserves_a_skipped_servers_evidence`] — the FLEET-VIEW
//!   safety property (iteration-1 C3/C5/T3 correction): with one server unreachable during a GC
//!   pass the deployed role DEFERS GC and leaves the skipped server's orphan record + fragment
//!   untouched; when the server returns on a later fully-reachable pass its garbage is reclaimed.
//!   A partial-fleet pass never mistakes "skipped" for "collected".
//! * [`deployed_role_reclaims_at_the_exact_grace_boundary`] — pins the INCLUSIVE reclaim boundary
//!   (`now == orphaned_at + grace` reclaims, `gc.rs:136` `>=`), which the ±1 ms probes leave open.
//! * [`deployed_role_defers_gc_when_the_operator_fleet_is_startup_partial`] — the #554 ITERATION-2
//!   correction: `connect_fleet` starts DEGRADED, so the `configured` slice can be SHORTER than the
//!   operator-wired fleet and `unreachable.is_empty()` (iteration-2's gate) passes on a partial
//!   fleet — retiring chunk-wide `pending:` evidence for a fragment a never-connected server holds
//!   (a permanent leak). The fix gates GC on the OPERATOR fleet size, so it DEFERS until every
//!   operator endpoint is visible. This is the test the hand-assembled-all-four T3 test cannot
//!   catch (the operator count is only ever equal to `configured.len()` there).
//! * [`deployed_retirement_leg_waits_for_the_whole_operator_fleet_and_keeps_completed_tombstones`]
//!   — the two inputs the loop hands the multipart retirement leg (#1013): the same
//!   `operator_fleet_size` it gates GC on, and the tombstone input `Retain`. On a startup-partial
//!   fleet an aborted upload's untrusted staging entry is held (`partial-fleet`, on the log sink),
//!   its record and every fragment of its chunk kept, while the leg still walks the trusted residue
//!   beside it; a completed upload's residue is walked away and its record kept. Once the whole
//!   fleet is visible the entry retires and the aborted upload ends; the completed one's record
//!   still stays. Passing the live fleet's length, or `Elapsed`, from the loop turns it red.
//! * [`deployed_loop_reaps_an_idle_open_upload_and_tears_it_down`] — the loop hands its
//!   reconstruction pass the multipart reaper (#625): an `Open` upload nobody aborted, with one
//!   committed part and a crashed part's residue, is left alone while it is hours old and, once
//!   the loop's clock is past the idle window, is fenced and torn down — every record gone, both
//!   byte sets marked, its admission slot released. Passing no reaper from the loop turns it red.
//! * [`deployed_loop_reaps_an_idle_upload_on_a_degraded_fleet_while_gc_is_deferred`] — the reaper
//!   does not wait for the whole fleet (#625, slice 625.2): three servers handed to the loop
//!   against an operator count of four, so GC is deferred on every pass, and leg A's idle upload
//!   (`multipart_reaper.rs`) is still fenced and torn down, both byte sets marked. Dispatching the
//!   reaper only inside GC's whole-fleet branch turns it red.
//! * [`no_fleet_loop_rides_out_seeded_store_faults_to_leg_a_teardown`] — the role's one interval
//!   driver with no fleet (`run_reaper_until`, slice 625.2), replayed under seeded store faults
//!   (`wyrd_testkit::Sim`): a pass a fault fails is logged and the next interval runs another, so
//!   leg A's teardown still completes and the loop returns `Ok`. Ending the loop on a store fault
//!   turns it red, and so does a loop that touches the store before it has polled its stop.
//! * [`no_fleet_loop_stops_when_a_newer_term_holds_the_zone`] — a deposed custodian's no-fleet
//!   loop returns `Err(Fenced)` from its first pass with the store as seeded.
//! * [`fleet_loop_polls_its_stop_before_its_first_pass`] — the same driver with a fleet polls its
//!   stop before the first pass probes the fleet (the role's Ctrl-C registers itself on that poll),
//!   and a stop requested by then ends the loop after exactly that one pass.
//! * [`deployed_run_loop_refuses_duplicate_endpoints`] /
//!   [`deployed_run_loop_refuses_duplicate_ids`] — the #554 ITERATION-4 correction: the fleet-
//!   identity uniqueness refusal used to guard ONLY the `--reconcile-after-restore` one-shot, but
//!   this bundle arms the DEPLOYED run-loop path with a DELETING GC pass. A duplicated endpoint (or
//!   two ids naming one box) fuses a server under two identities, so a live fragment protected as
//!   `(A, frag)` is unreferenced as `(B, frag)` and GC would DELETE it. Both tests drive the REAL
//!   `cli::cmd_custodian` entry on the run-loop path (no `--reconcile-after-restore`) and assert it
//!   REFUSES before dialing. RED on base (the run-loop path had no refusal, so the fused fleet
//!   reached connect_fleet); GREEN with the hoisted refusal.
//! * [`deployed_run_loop_refuses_a_value_after_the_valueless_gc_flag`] — the Codex P2 hole in the
//!   valueless parse: `--gc-expired-pending false` reads as the flag PRESENT plus an ignored
//!   positional, arming the unsafe sweep the caller tried to disable. The entry refuses any stray
//!   positional (the command has no positional grammar) before it arms anything.
//!
//! RED SHAPE (honest): closing the startup-partial hole REQUIRES the operator fleet size at the
//! run-loop entry, so `run_reconstruction_until` gains an `operator_fleet_size` parameter (the
//! `#551` restore pass reads the same operator count, `cli.rs:961-975`). The new signature means
//! this file's red leg on a fully-reverted base is a COMPILE ERROR (E0061), not an assertion —
//! flagged in build-notes as the brief's Test-file note requires. The behavioural binding is shown
//! separately by reverting ONLY the gate (`fleet.len() == operator_fleet_size` →
//! `unreachable.is_empty()`): the startup-partial test then fails by ASSERTION.
//!
//! Its own test binary (a separate process): `tracing` caches per-callsite interest in
//! process-global state, so this role's metric callsites must not be raced by a no-subscriber
//! sibling test — the same isolation reason `custodian_day_one.rs` documents.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, bucket_key, orphan_key, parse_orphan_key, pending_key, ChunkRef, EcScheme, InodeId,
    PendingEntry, SegmentNonce,
};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::complete::{
    complete_upload, CompleteRequest, NamedParts, PublishRequest,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    decode_admission_record, decode_part_record, mpu_key, part_key, psum_key, sidx_key,
    AbortOutcome, Budget, CompleteOutcome, CreateOutcome, Digest, OwnedEntry, PartNumber,
    PartSummary, StagedPlacement, UploadId, MPUCTL_KEY,
};
use wyrd_core::placement::Topology;
use wyrd_core::write::write_new_object_placed;
use wyrd_custodian::{Custodian, ExpiredPendingPolicy, FencedZone, ReconcileError};
use wyrd_server::custodian::{ConfiguredDServer, CustodianService};
use wyrd_telemetry::{DurabilityTelemetry, ExporterConfig};
use wyrd_testkit::Sim;
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, CommitUnknownResult, DServerId, FragmentId, Health,
    MetadataStore, PlacementChunkStore, Result, WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the loop is proven over the seams) ----

/// A trivial in-memory metadata store (mirrors `custodian_day_one.rs`).
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

    // The required paginated read (#634): a test double needs *a* body, not a
    // backend's — the dev-only testkit helper pages over this store's own `scan`
    // (and therefore inherits `SCAN_CAP`, which a backend may not).
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
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

/// One D server's fragment bytes — a deliberately dumb `ChunkStore` holding the real stored
/// fragment bytes, so GC's `delete_fragment` reclaim is observable through `get_fragment`. Its
/// reachability is toggleable (`healthy`) so a test can take the server offline for a GC pass and
/// bring it back on a later one, driving the deployed fleet-view (`live_reconstruction_view`
/// drops a server whose `health()` errs). Its probes are counted: the deployed loop probes every
/// configured server once a pass, so the count is the passes begun.
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
    healthy: AtomicBool,
    probes: AtomicU64,
}

impl Default for MemDServer {
    fn default() -> Self {
        Self {
            frags: Mutex::new(HashMap::new()),
            healthy: AtomicBool::new(true),
            probes: AtomicU64::new(0),
        }
    }
}

impl MemDServer {
    /// Take this server reachable / unreachable for the reachability probe (`health()`).
    fn set_reachable(&self, reachable: bool) {
        self.healthy.store(reachable, Ordering::SeqCst);
    }

    /// How many reachability probes this server has answered.
    fn probes(&self) -> u64 {
        self.probes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ChunkStore for MemDServer {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.frags.lock().unwrap().insert(id, fragment);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&id).cloned())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.frags.lock().unwrap().keys().copied().collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.frags.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        if self.healthy.load(Ordering::SeqCst) {
            Ok(Health::Healthy)
        } else {
            // A plain transport error, the shape a dead endpoint raises — the reachability probe
            // reads it as "unreachable this pass" and drops the server from the live fleet.
            Err("d-server unreachable: connection refused".into())
        }
    }
}

/// A **placement-aware** fleet over several [`MemDServer`]s: it routes `_at` calls to the D
/// server the placement record names, so the write fan-out resolves each fragment from its
/// recorded location (mirrors `custodian_day_one.rs`).
struct Fleet<'a> {
    servers: Vec<(DServerId, &'a MemDServer)>,
}

impl<'a> Fleet<'a> {
    fn store(&self, dserver: DServerId) -> Option<&'a MemDServer> {
        self.servers
            .iter()
            .find(|(id, _)| *id == dserver)
            .map(|(_, s)| *s)
    }
}

#[async_trait]
impl ChunkStore for Fleet<'_> {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        if let Some(store) = self.store(DServerId::from(id.index)) {
            store.put_fragment(id, fragment, deadline_millis).await?;
        }
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        for (_, store) in &self.servers {
            if let Some(bytes) = store.get_fragment(id).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        let mut all = Vec::new();
        for (_, store) in &self.servers {
            all.extend(store.list_fragments().await?);
        }
        Ok(all)
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        for (_, store) in &self.servers {
            store.delete_fragment(id).await?;
        }
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

#[async_trait]
impl PlacementChunkStore for Fleet<'_> {
    async fn get_fragment_at(&self, dserver: DServerId, id: FragmentId) -> Result<Option<Bytes>> {
        match self.store(dserver) {
            Some(store) => store.get_fragment(id).await,
            None => Ok(None),
        }
    }

    async fn put_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        if let Some(store) = self.store(dserver) {
            store.put_fragment(id, fragment, deadline_millis).await?;
        }
        Ok(())
    }
}

// ---- helpers ----

const ROOT: InodeId = 0;

/// A four-domain topology A..D (servers 0..3).
fn four_domains() -> Topology {
    let mut t = Topology::default();
    t.register(0, "A")
        .register(1, "B")
        .register(2, "C")
        .register(3, "D");
    t
}

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-reconstruction")
        .await
        .unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

/// Erase a concrete `Arc<S>` store to the owned `Arc<dyn ChunkStore>` the fleet holds.
fn dyn_store<S: ChunkStore + 'static>(store: &Arc<S>) -> Arc<dyn ChunkStore> {
    store.clone()
}

/// The real production fleet-input the role is handed: every configured D-server, keyed by its
/// operator-supplied stable id + failure domain (mirrors `custodian_day_one.rs`).
fn configured(servers: [(DServerId, &str, Arc<dyn ChunkStore>); 4]) -> Vec<ConfiguredDServer> {
    servers
        .into_iter()
        .map(|(id, dom, store)| ConfiguredDServer {
            id,
            failure_domain: dom.to_string(),
            store,
        })
        .collect()
}

/// Write one RS(2,1) object via the real write path under an explicit inode/name/chunk id — its
/// 3 fragments land on servers 0,1,2 (domains A,B,C), fragment index i on server i.
async fn write_rs_2_1_as(
    meta: &MemMeta,
    fleet: &Fleet<'_>,
    inode_id: InodeId,
    name: &str,
    chunk_id: ChunkId,
) {
    let data = format!("reclaim erasure-coded chunk {chunk_id:#x}, every byte of it").into_bytes();
    let topo = four_domains();
    let outcome = write_new_object_placed(
        meta,
        fleet,
        ROOT,
        name,
        inode_id,
        &data,
        data.len(),
        EcScheme::ReedSolomon { k: 2, m: 1 },
        &topo,
        || 0,
        1_000,
        || chunk_id,
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

/// Is fragment `index` of `chunk` still physically present on `store`?
async fn present(store: &MemDServer, chunk: ChunkId, index: u16) -> bool {
    store
        .get_fragment(FragmentId { chunk, index })
        .await
        .unwrap()
        .is_some()
}

/// Does the metadata store still carry the orphan grace record for fragment `index` of `chunk`
/// placed on `dserver`? This is the evidence a later GC pass reads; "skipped" must never delete it.
async fn orphan_record_present(
    meta: &MemMeta,
    dserver: DServerId,
    chunk: ChunkId,
    index: u16,
) -> bool {
    meta.get(&orphan_key(dserver, FragmentId { chunk, index }))
        .await
        .unwrap()
        .is_some()
}

// The lease TTL the role derives its grace window from (`cli.rs:68` / `custodian.rs`
// GC_GRACE_WINDOW_MILLIS = LEASE_TTL_MILLIS). Kept in step with that derivation so the test
// pins the deployed mechanism, not a re-invented constant.
const GRACE_MILLIS: u64 = 60_000;
const ORPHANED_AT: u64 = 1_000;
const DOOMED_CHUNK: ChunkId = 0xDEAD_BEEF;
const DOOMED_INODE: InodeId = 1;
const LIVE_CHUNK: ChunkId = 0x0000_1111;
const LIVE_INODE: InodeId = 2;

/// Build four owned in-memory D-servers.
fn four_servers() -> (
    Arc<MemDServer>,
    Arc<MemDServer>,
    Arc<MemDServer>,
    Arc<MemDServer>,
) {
    (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    )
}

/// Drive the REAL production run loop over the fleet with a fixed logical clock, for a short
/// window (a handful of passes), then shut down — the exact wiring `cli::cmd_custodian` runs
/// with NO extra flags: the expired-pending input stays [`ExpiredPendingPolicy::Defer`]red
/// (the deployed default — see `cmd_custodian`'s `--gc-expired-pending` parse). The operator
/// fleet size is the number of servers handed in (a WHOLE, fully-connected fleet);
/// [`drive_deployed_loop_operator`] drives the startup-partial case where the operator wired more
/// endpoints than the loop can see.
async fn drive_deployed_loop(meta: &MemMeta, servers: &[ConfiguredDServer], now_millis: u64) {
    drive_deployed_loop_operator(
        meta,
        servers,
        servers.len(),
        ExpiredPendingPolicy::Defer,
        now_millis,
    )
    .await;
}

/// [`drive_deployed_loop`], but as `wyrd custodian --gc-expired-pending` wires it: the operator
/// has attested the backend takes no writes, so GC's expired-pending input is ARMED
/// ([`ExpiredPendingPolicy::Reclaim`]).
async fn drive_deployed_loop_armed(meta: &MemMeta, servers: &[ConfiguredDServer], now_millis: u64) {
    drive_deployed_loop_operator(
        meta,
        servers,
        servers.len(),
        ExpiredPendingPolicy::Reclaim,
        now_millis,
    )
    .await;
}

/// Drive the REAL production run loop with the operator-configured fleet size passed EXPLICITLY,
/// so a test can make it LARGER than `servers.len()` — the startup-partial fleet the deployed role
/// sees when `connect_fleet` dropped a peer that was unreachable at boot (`cli.rs`). This is the
/// exact input path `cmd_custodian` produces: `run_reconstruction_over_backend` hands the loop the
/// degraded `configured` slice plus `endpoints.len()` (the operator count). The loop's GC pass
/// must gate on the operator count, never on `servers.len()` (#554 iteration-2 correction).
/// `expired_pending` is the operator's `--gc-expired-pending` decision, exactly as `cmd_custodian`
/// threads it.
async fn drive_deployed_loop_operator(
    meta: &MemMeta,
    servers: &[ConfiguredDServer],
    operator_fleet_size: usize,
    expired_pending: ExpiredPendingPolicy,
    now_millis: u64,
) {
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);
    let shutdown = async { tokio::time::sleep(Duration::from_millis(60)).await };
    service
        .run_reconstruction_until(
            &zone,
            &custodian,
            meta,
            servers,
            operator_fleet_size,
            expired_pending,
            Duration::from_millis(10),
            move || now_millis,
            shutdown,
        )
        .await
        .expect("the deployed run loop survives and exits cleanly at shutdown");
}

// ---- the deployed role RECLAIMS orphaned bytes once the grace window has elapsed ----

#[tokio::test]
async fn deployed_role_reclaims_orphaned_bytes_after_grace_elapses() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = four_servers();
    {
        let fleet = Fleet {
            servers: vec![
                (0, d0.as_ref()),
                (1, d1.as_ref()),
                (2, d2.as_ref()),
                (3, d3.as_ref()),
            ],
        };
        write_rs_2_1_as(&meta, &fleet, DOOMED_INODE, "doomed", DOOMED_CHUNK).await;
        write_rs_2_1_as(&meta, &fleet, LIVE_INODE, "live", LIVE_CHUNK).await;
    }

    // DELETE the doomed object through the production write path: `unlink` removes its metadata
    // and, in the SAME atomic commit, writes an orphan grace record (keyed by the placed
    // D-server) for every fragment it placed — the marks GC reads (metadata.rs:369-415).
    let unlinked = metadata::unlink(&meta, ROOT, "doomed", ORPHANED_AT)
        .await
        .unwrap()
        .expect("the doomed object existed and was unlinked");
    assert_eq!(unlinked.outcome, CommitOutcome::Committed);

    // Sanity: the orphaned bytes are STILL on the D-servers immediately after the delete — the
    // write path marks, never deletes; GC is what reclaims them.
    assert!(present(&d0, DOOMED_CHUNK, 0).await);
    assert!(present(&d1, DOOMED_CHUNK, 1).await);
    assert!(present(&d2, DOOMED_CHUNK, 2).await);

    // The production fleet input: every configured D-server, all reachable.
    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    // Advance the logical clock PAST the grace window (orphaned_at + grace), then drive the
    // deployed loop. On base the run loop never runs GC, so nothing is reclaimed — RED. With the
    // fix its GC pass reclaims the orphaned fragments — GREEN.
    let past_grace = ORPHANED_AT + GRACE_MILLIS + 1;
    drive_deployed_loop(&meta, &servers, past_grace).await;

    // The orphaned fragments are physically GONE from the D-servers.
    assert!(
        !present(&d0, DOOMED_CHUNK, 0).await,
        "the deployed GC pass reclaimed the orphaned fragment on server 0 (RED on base: it never runs GC)"
    );
    assert!(
        !present(&d1, DOOMED_CHUNK, 1).await,
        "the deployed GC pass reclaimed the orphaned fragment on server 1"
    );
    assert!(
        !present(&d2, DOOMED_CHUNK, 2).await,
        "the deployed GC pass reclaimed the orphaned fragment on server 2"
    );

    // The still-LIVE object loses nothing — its fragments are committed-referenced, so GC's
    // safety gate protects every one of them.
    assert!(
        present(&d0, LIVE_CHUNK, 0).await
            && present(&d1, LIVE_CHUNK, 1).await
            && present(&d2, LIVE_CHUNK, 2).await,
        "GC never reclaims a referenced fragment — the live object survives intact"
    );
}

// ---- and it does NOT reclaim before the grace window elapses (reader-safe no-reclaim-before) ----

#[tokio::test]
async fn deployed_role_keeps_orphaned_bytes_within_the_grace_window() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = four_servers();
    {
        let fleet = Fleet {
            servers: vec![
                (0, d0.as_ref()),
                (1, d1.as_ref()),
                (2, d2.as_ref()),
                (3, d3.as_ref()),
            ],
        };
        write_rs_2_1_as(&meta, &fleet, DOOMED_INODE, "doomed", DOOMED_CHUNK).await;
    }

    let unlinked = metadata::unlink(&meta, ROOT, "doomed", ORPHANED_AT)
        .await
        .unwrap()
        .expect("the doomed object existed and was unlinked");
    assert_eq!(unlinked.outcome, CommitOutcome::Committed);

    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    // Drive the loop with the clock still INSIDE the grace window (orphaned_at + grace - 1): the
    // reader-safe window has NOT elapsed, so no fragment may be reclaimed yet.
    let within_grace = ORPHANED_AT + GRACE_MILLIS - 1;
    drive_deployed_loop(&meta, &servers, within_grace).await;

    assert!(
        present(&d0, DOOMED_CHUNK, 0).await
            && present(&d1, DOOMED_CHUNK, 1).await
            && present(&d2, DOOMED_CHUNK, 2).await,
        "no orphaned fragment is reclaimed before its reader-safe grace window elapses"
    );
}

// ---- the deployed role reclaims GC's SECOND input: expired pending-lease garbage ----

#[tokio::test]
async fn armed_deployed_role_reclaims_expired_pending_lease_garbage() {
    // The bytes a crashed write fan-out leaves are collectable via the pending ledger, not the
    // orphan ledger: a `pending:<chunk>` lease with no committed inode. Once the lease expires,
    // GC reclaims the leased fragments (`gc.rs`). This pins that the deployed loop honours that
    // input, not only delete-orphans — the brief's "expired pending leases are likewise GC's
    // input". The lease here is genuinely abandoned, which the role cannot verify — "expired" is
    // untrustworthy while any producer stamps logical-clock leases (#557) — so this input runs
    // ONLY under the operator's `--gc-expired-pending` attestation that no writer is live:
    // the loop is driven ARMED. The deployed DEFAULT defers it —
    // [`deployed_role_defers_expired_pending_garbage_by_default`].
    const LEASED_CHUNK: ChunkId = 0x0AB1_DEAD;
    const LEASE_EXPIRY: u64 = 500;

    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = four_servers();

    // A crashed fan-out: two fragments written to their placed D-servers, but the write never
    // committed an inode — only the pending lease remains. Placement index i → server i.
    d0.put_fragment(
        FragmentId {
            chunk: LEASED_CHUNK,
            index: 0,
        },
        Bytes::from_static(b"leased fan-out fragment 0"),
        None,
    )
    .await
    .unwrap();
    d1.put_fragment(
        FragmentId {
            chunk: LEASED_CHUNK,
            index: 1,
        },
        Bytes::from_static(b"leased fan-out fragment 1"),
        None,
    )
    .await
    .unwrap();
    // Record the pending lease exactly as the write path's ledger does.
    meta.commit(WriteBatch::new().put(
        pending_key(LEASED_CHUNK),
        metadata::encode(&PendingEntry {
            lease_expiry_millis: LEASE_EXPIRY,
            owner: None,
            staged: None,
        }),
    ))
    .await
    .unwrap();

    assert!(present(&d0, LEASED_CHUNK, 0).await);
    assert!(present(&d1, LEASED_CHUNK, 1).await);

    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    // Drive the deployed loop ARMED with the clock PAST the lease expiry: the lease is expired,
    // so its leased fan-out bytes are collectable. RED on base (the loop never runs GC); GREEN
    // with the fix.
    drive_deployed_loop_armed(&meta, &servers, LEASE_EXPIRY + 1).await;

    assert!(
        !present(&d0, LEASED_CHUNK, 0).await && !present(&d1, LEASED_CHUNK, 1).await,
        "the armed GC pass reclaimed the expired pending-lease garbage (RED on base: no GC runs)"
    );
    assert!(
        meta.get(&pending_key(LEASED_CHUNK))
            .await
            .unwrap()
            .is_none(),
        "the swept pending-ledger entry is retired once its leased bytes are reclaimed"
    );
}

// ---- the deployed DEFAULT never trusts "expired": pending garbage is deferred, not swept ----

#[tokio::test]
async fn deployed_role_defers_expired_pending_garbage_by_default() {
    // The #557 mid-flight hazard, pinned as a GATE rather than a doc warning: the CLI write path
    // stamps `pending:` leases from a fixed logical clock (`cli.rs` `NOW_MILLIS = 0`, so
    // `lease_expiry = LEASE_TTL_MILLIS`), and against the deployed role's wall clock that lease
    // reads as expired WHILE THE WRITE IS STILL IN FLIGHT. Swept, the fan-out is deleted and the
    // writer then commits a chunk map over missing bytes. So without `--gc-expired-pending` the
    // deployed role must treat every `pending:` entry — however expired it looks — as DEFERRED:
    // fragments and the lease survive untouched. This is exactly that in-flight shape: a lease
    // stamped at logical zero, a custodian clock far past it. Fails if the deployed default ever
    // regresses to `Reclaim`.
    const LEASED_CHUNK: ChunkId = 0x0AB1_F11E;
    // The CLI's stamp: `NOW_MILLIS (0) + LEASE_TTL_MILLIS` — one minute past the Unix epoch.
    const LOGICAL_ZERO_LEASE_EXPIRY: u64 = 60_000;
    // A deployed custodian's wall clock: some 2026 instant, eons past the logical-zero expiry.
    const WALL_CLOCK_NOW: u64 = 1_780_000_000_000;

    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = four_servers();

    // The mid-flight fan-out: fragments landed, no inode committed yet, lease live from the
    // writer's point of view.
    d0.put_fragment(
        FragmentId {
            chunk: LEASED_CHUNK,
            index: 0,
        },
        Bytes::from_static(b"in-flight fan-out fragment 0"),
        None,
    )
    .await
    .unwrap();
    d1.put_fragment(
        FragmentId {
            chunk: LEASED_CHUNK,
            index: 1,
        },
        Bytes::from_static(b"in-flight fan-out fragment 1"),
        None,
    )
    .await
    .unwrap();
    meta.commit(WriteBatch::new().put(
        pending_key(LEASED_CHUNK),
        metadata::encode(&PendingEntry {
            lease_expiry_millis: LOGICAL_ZERO_LEASE_EXPIRY,
            owner: None,
            staged: None,
        }),
    ))
    .await
    .unwrap();

    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    // The DEFAULT deployed loop (no --gc-expired-pending), whole fleet visible, clock far past
    // the stamped expiry — the exact shape that must NOT be trusted.
    drive_deployed_loop(&meta, &servers, WALL_CLOCK_NOW).await;

    assert!(
        present(&d0, LEASED_CHUNK, 0).await && present(&d1, LEASED_CHUNK, 1).await,
        "the deployed default DEFERS expired-pending garbage: a logical-zero-stamped lease reads \
         as expired against the wall clock while its write is still in flight, so sweeping it \
         would delete a mid-flight fan-out (#557)"
    );
    assert!(
        meta.get(&pending_key(LEASED_CHUNK))
            .await
            .unwrap()
            .is_some(),
        "the pending lease survives untouched — deferred is never mistaken for collected"
    );
}

// ---- FLEET-VIEW: a skipped (unreachable) server's evidence survives for a later pass ----

#[tokio::test]
async fn deployed_role_defers_gc_and_preserves_a_skipped_servers_evidence() {
    // The iteration-1 C3/C5/T3 correction: a GC pass over a PARTIAL (reachable-only) fleet must
    // never retire evidence for garbage a SKIPPED server still holds. The deployed role defers GC
    // whenever any server is unreachable, so the skipped server's orphan record + fragment are
    // preserved untouched and reclaimed on a later fully-reachable pass. "Skipped" is never
    // mistaken for "collected".
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = four_servers();
    {
        let fleet = Fleet {
            servers: vec![
                (0, d0.as_ref()),
                (1, d1.as_ref()),
                (2, d2.as_ref()),
                (3, d3.as_ref()),
            ],
        };
        write_rs_2_1_as(&meta, &fleet, DOOMED_INODE, "doomed", DOOMED_CHUNK).await;
    }
    let unlinked = metadata::unlink(&meta, ROOT, "doomed", ORPHANED_AT)
        .await
        .unwrap()
        .expect("the doomed object existed and was unlinked");
    assert_eq!(unlinked.outcome, CommitOutcome::Committed);

    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    // PASS WINDOW 1 — server 2 (which holds the doomed fragment index 2) is UNREACHABLE. The
    // deployed role's reachability probe drops it, so GC is DEFERRED: nothing is reclaimed and
    // EVERY orphan record survives — the clock is already past grace, proving it is the skip, not
    // the grace window, that stays GC's hand.
    d2.set_reachable(false);
    let past_grace = ORPHANED_AT + GRACE_MILLIS + 1;
    drive_deployed_loop(&meta, &servers, past_grace).await;

    assert!(
        present(&d2, DOOMED_CHUNK, 2).await,
        "the SKIPPED server's orphaned fragment is preserved (GC deferred while it was unreachable)"
    );
    assert!(
        orphan_record_present(&meta, 2, DOOMED_CHUNK, 2).await,
        "the SKIPPED server's orphan grace record survives — its evidence is not retired as if collected"
    );
    // The reachable servers' evidence is preserved too (GC deferred fleet-wide, not partially run).
    assert!(
        present(&d0, DOOMED_CHUNK, 0).await && present(&d1, DOOMED_CHUNK, 1).await,
        "no fragment is reclaimed on any server while the fleet is partial"
    );

    // PASS WINDOW 2 — server 2 RETURNS. Now the fleet is whole, GC runs, and the garbage that was
    // preserved across the outage — including the once-skipped server's fragment — is reclaimed.
    d2.set_reachable(true);
    drive_deployed_loop(&meta, &servers, past_grace).await;

    assert!(
        !present(&d0, DOOMED_CHUNK, 0).await
            && !present(&d1, DOOMED_CHUNK, 1).await
            && !present(&d2, DOOMED_CHUNK, 2).await,
        "once the fleet is whole again the deferred garbage — the skipped server's copy included — is reclaimed"
    );
    assert!(
        !orphan_record_present(&meta, 2, DOOMED_CHUNK, 2).await,
        "the consumed orphan record is retired only once its fragment is actually reclaimed"
    );
}

// ---- the reclaim boundary is INCLUSIVE: now == orphaned_at + grace reclaims (gc.rs:136) ----

#[tokio::test]
async fn deployed_role_reclaims_at_the_exact_grace_boundary() {
    // gc.rs:136 reclaims when `now_millis >= orphaned_at + grace_window_millis` — an INCLUSIVE
    // boundary. The `within_grace` (…- 1) and `past_grace` (…+ 1) tests probe either side at ±1 ms
    // but leave the boundary itself unpinned; a regression to a strict `>` would slip past both.
    // This pins the exact instant: driven with the clock at PRECISELY `orphaned_at + grace`, the
    // deployed loop reclaims (adversary conformance nit, #554 iteration-2).
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = four_servers();
    {
        let fleet = Fleet {
            servers: vec![
                (0, d0.as_ref()),
                (1, d1.as_ref()),
                (2, d2.as_ref()),
                (3, d3.as_ref()),
            ],
        };
        write_rs_2_1_as(&meta, &fleet, DOOMED_INODE, "doomed", DOOMED_CHUNK).await;
    }
    let unlinked = metadata::unlink(&meta, ROOT, "doomed", ORPHANED_AT)
        .await
        .unwrap()
        .expect("the doomed object existed and was unlinked");
    assert_eq!(unlinked.outcome, CommitOutcome::Committed);

    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    // Clock at EXACTLY the boundary: orphaned_at + grace. The inclusive `>=` reclaims here.
    let at_boundary = ORPHANED_AT + GRACE_MILLIS;
    drive_deployed_loop(&meta, &servers, at_boundary).await;

    assert!(
        !present(&d0, DOOMED_CHUNK, 0).await
            && !present(&d1, DOOMED_CHUNK, 1).await
            && !present(&d2, DOOMED_CHUNK, 2).await,
        "at now == orphaned_at + grace the reader-safe window has elapsed (inclusive), so the \
         deployed GC pass reclaims (gc.rs:136 `>=`)"
    );
}

// ---- STARTUP-PARTIAL FLEET: the operator wired more endpoints than the loop can see ----

#[tokio::test]
async fn deployed_role_defers_gc_when_the_operator_fleet_is_startup_partial() {
    // The #554 ITERATION-2 correction. `connect_fleet` starts DEGRADED: a D-server unreachable at
    // BOOT is silently dropped, so the `configured` slice the loop receives is already SHORTER than
    // the operator-wired fleet — and `live_reconstruction_view(configured)` then returns an EMPTY
    // `unreachable`, because it only probes the servers that DID connect. Gating GC on
    // `unreachable.is_empty()` (iteration-2's gate) therefore PASSES on this partial fleet and the
    // first GC pass retires CHUNK-WIDE `pending:` evidence for a chunk a never-connected server
    // still holds a fragment of — stranding it forever (a permanent silent leak). The fix gates GC
    // on the OPERATOR fleet size, so it DEFERS until every operator endpoint is visible.
    //
    // Demonstrated on GC's expired-pending input precisely because that input retires evidence
    // chunk-wide (`gc.rs:155-167`) — the input the startup-partial hazard actually strands.
    const LEASED_CHUNK: ChunkId = 0x0FF1_0FF1;
    const LEASE_EXPIRY: u64 = 500;

    let meta = MemMeta::default();
    let (d0, d1, d2) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    // A crashed fan-out: fragments on servers 0,1,2 under a single chunk-wide pending lease.
    for (idx, d) in [(0u16, &d0), (1, &d1), (2, &d2)] {
        d.put_fragment(
            FragmentId {
                chunk: LEASED_CHUNK,
                index: idx,
            },
            Bytes::from(format!("leased fan-out fragment {idx}")),
            None,
        )
        .await
        .unwrap();
    }
    meta.commit(WriteBatch::new().put(
        pending_key(LEASED_CHUNK),
        metadata::encode(&PendingEntry {
            lease_expiry_millis: LEASE_EXPIRY,
            owner: None,
            staged: None,
        }),
    ))
    .await
    .unwrap();

    let build = |servers: &[(DServerId, &str, &Arc<MemDServer>)]| -> Vec<ConfiguredDServer> {
        servers
            .iter()
            .map(|(id, dom, store)| ConfiguredDServer {
                id: *id,
                failure_domain: dom.to_string(),
                store: dyn_store(store),
            })
            .collect()
    };

    // The operator wired THREE endpoints (servers 0,1,2). Server 2 — which holds fragment index 2 —
    // was unreachable at STARTUP, so `connect_fleet` dropped it and the loop's `configured` holds
    // only servers 0,1. Every connected server is reachable, so `unreachable.is_empty()` is TRUE:
    // iteration-2's gate would run GC over the partial fleet, reclaim fragments 0,1, retire the
    // chunk-wide pending entry, and strand server 2's fragment forever.
    const OPERATOR_FLEET_SIZE: usize = 3;
    let partial = build(&[(0, "A", &d0), (1, "B", &d1)]);
    let past_expiry = LEASE_EXPIRY + 1;
    // ARMED (`--gc-expired-pending`): the fleet gate must hold even when the operator has
    // attested the pending input safe — arming is no license to sweep a partial fleet.
    drive_deployed_loop_operator(
        &meta,
        &partial,
        OPERATOR_FLEET_SIZE,
        ExpiredPendingPolicy::Reclaim,
        past_expiry,
    )
    .await;

    // GC DEFERRED: nothing reclaimed on ANY server, and the chunk-wide pending evidence survives —
    // so the fragment on the still-absent server 2 is still reclaimable on a later whole-fleet pass.
    assert!(
        present(&d0, LEASED_CHUNK, 0).await && present(&d1, LEASED_CHUNK, 1).await,
        "GC deferred on the startup-partial fleet: no reachable fragment is swept (RED under the \
         iteration-2 `unreachable.is_empty()` gate, which sweeps them and strands server 2)"
    );
    assert!(
        present(&d2, LEASED_CHUNK, 2).await,
        "the startup-omitted server's fragment is untouched — it was never in the swept fleet"
    );
    assert!(
        meta.get(&pending_key(LEASED_CHUNK))
            .await
            .unwrap()
            .is_some(),
        "the CHUNK-WIDE pending evidence is preserved — never retired while the fleet is partial, \
         so server 2's fragment stays reclaimable (the leak the iteration-2 gate would cause)"
    );

    // Server 2 comes back: the operator now sees its WHOLE fleet (3 == 3). GC runs and reclaims the
    // whole chunk's garbage, retiring the pending entry only now that every copy was swept.
    let whole = build(&[(0, "A", &d0), (1, "B", &d1), (2, "C", &d2)]);
    drive_deployed_loop_operator(
        &meta,
        &whole,
        OPERATOR_FLEET_SIZE,
        ExpiredPendingPolicy::Reclaim,
        past_expiry,
    )
    .await;

    assert!(
        !present(&d0, LEASED_CHUNK, 0).await
            && !present(&d1, LEASED_CHUNK, 1).await
            && !present(&d2, LEASED_CHUNK, 2).await,
        "once the whole operator fleet is visible the deferred pending garbage is reclaimed on \
         every server, the once-absent server included"
    );
    assert!(
        meta.get(&pending_key(LEASED_CHUNK))
            .await
            .unwrap()
            .is_none(),
        "the pending ledger entry is retired only after the whole fleet was swept"
    );
}

// ---- FLEET IDENTITY: the deploying (GC-armed) run-loop path REFUSES a fused fleet ----
//
// #554 arms the DEPLOYED run loop (`wyrd custodian` WITHOUT `--reconcile-after-restore`) with a
// DELETING GC pass. Two identities for one physical box — a duplicated `--endpoints`, or two
// `--ids` naming the same server — fuse it into a phantom fleet: a LIVE fragment protected as
// `(A, frag)` is unreferenced when the very same bytes are seen as `(B, frag)`, so GC would reclaim
// and DELETE it. The uniqueness refusal used to live ONLY inside the `--reconcile-after-restore`
// one-shot; on the run-loop path it was absent, so the fused fleet reached the GC sweep unvalidated
// (iteration-4 adversary [impl], confirmed). These two tests drive the REAL production entry
// (`cli::cmd_custodian`) on the run-loop path and assert it REFUSES before ever dialing the fleet.
//
// Plain `#[test]` (NOT `#[tokio::test]`): `cmd_custodian` builds its OWN multi-thread runtime and
// `block_on`s it, which panics if called from within an ambient tokio runtime. The refusal returns
// BEFORE `connect_fleet`, so no endpoint is dialed on the GREEN (fixed) path. On the reverted base
// the run-loop path has no refusal, so the fused fleet instead reaches `connect_fleet`, fails to
// dial the (unreachable) endpoints, and panics on the empty fleet — caught here as a NON-duplicate
// failure, which is the honest RED for these tests.
fn custodian_args(pairs: &[(&str, &str)]) -> Vec<String> {
    let mut args = Vec::new();
    for (flag, value) in pairs {
        args.push(format!("--{flag}"));
        args.push((*value).to_string());
    }
    args
}

#[test]
fn deployed_run_loop_refuses_duplicate_endpoints() {
    // Duplicate --endpoints on the run-loop path (no --reconcile-after-restore): one box under two
    // identities. GC would see its live fragment as unreferenced under the second identity and
    // DELETE it. The refusal must fire before `connect_fleet` — never dial a fused fleet.
    let args = custodian_args(&[
        ("metadata-backend", "redb"),
        ("endpoints", "http://127.0.0.1:1,http://127.0.0.1:1"),
        ("ids", "7,9"),
        ("failure-domains", "a,b"),
        ("connect-timeout-secs", "1"),
    ]);

    let result = std::panic::catch_unwind(|| wyrd_server::cli::cmd_custodian(&args));
    match result {
        Ok(Ok(_)) => panic!(
            "cmd_custodian ACCEPTED duplicate --endpoints on the run-loop path — the GC pass would \
             delete a live fragment fused under the second identity"
        ),
        Ok(Err(e)) => {
            let msg = e.to_string();
            assert!(
                msg.contains("--endpoints contains duplicates"),
                "the run-loop path must REFUSE duplicate --endpoints with the fleet-identity error \
                 (protecting the GC sweep); got a different error instead: {msg}"
            );
        }
        Err(_) => panic!(
            "cmd_custodian did NOT refuse duplicate --endpoints before dialing (RED on base): the \
             run-loop path reached connect_fleet and panicked on the unreachable fused fleet rather \
             than refusing the duplicate up front"
        ),
    }
}

#[test]
fn deployed_run_loop_refuses_duplicate_ids() {
    // Duplicate --ids on the run-loop path: distinct endpoints, but two servers fused under one
    // stable id. Fragments are keyed by (d-server id, fragment), so the repair loop could displace
    // a fragment between the two boxes and GC would reclaim against a fleet that does not exist.
    let args = custodian_args(&[
        ("metadata-backend", "redb"),
        ("endpoints", "http://127.0.0.1:1,http://127.0.0.1:2"),
        ("ids", "7,7"),
        ("failure-domains", "a,b"),
        ("connect-timeout-secs", "1"),
    ]);

    let result = std::panic::catch_unwind(|| wyrd_server::cli::cmd_custodian(&args));
    match result {
        Ok(Ok(_)) => panic!(
            "cmd_custodian ACCEPTED duplicate --ids on the run-loop path — two boxes fused under one \
             id lets the repair loop displace a fragment and GC reclaim against a phantom fleet"
        ),
        Ok(Err(e)) => {
            let msg = e.to_string();
            assert!(
                msg.contains("--ids contains duplicates"),
                "the run-loop path must REFUSE duplicate --ids with the fleet-identity error; got a \
                 different error instead: {msg}"
            );
        }
        Err(_) => panic!(
            "cmd_custodian did NOT refuse duplicate --ids before dialing (RED on base): the run-loop \
             path reached connect_fleet and panicked on the unreachable fleet rather than refusing \
             the duplicate id up front"
        ),
    }
}

#[test]
fn deployed_run_loop_refuses_a_value_after_the_valueless_gc_flag() {
    // `--gc-expired-pending false`: the operator (or a wrapper rendering booleans) is trying to
    // DISABLE the unsafe sweep, but the valueless-flag parse reads the flag as PRESENT and leaves
    // `false` as an ignored positional — arming exactly the mode being refused. `wyrd custodian`
    // has no positional grammar, so the entry must reject any stray token outright, before it
    // arms anything.
    let args: Vec<String> = [
        "--metadata-backend",
        "redb",
        "--gc-expired-pending",
        "false",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let err = wyrd_server::cli::cmd_custodian(&args).expect_err(
        "cmd_custodian ACCEPTED a value after the valueless --gc-expired-pending flag — \
         `--gc-expired-pending false` would silently ARM the expired-pending sweep",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("unexpected positional argument `false`"),
        "the refusal must name the stray token so the operator sees what was ignored; got: {msg}"
    );
}

// ---- the RETIREMENT leg reads the same operator fleet size, and keeps completed tombstones ----

const BUCKET: &str = "photos";
const BUCKET_MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
const RS21: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
const RS42: EcScheme = EcScheme::ReedSolomon { k: 4, m: 2 };
/// The deployed loop's clock for these passes: fixed, one source for every read.
const LOOP_NOW: u64 = 10_000_000;

/// A `MakeWriter` appending everything the role's log layer writes to a shared buffer (as in
/// `custodian_day_one.rs`), so the test reads what the role emitted.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
    type Writer = Self;
    fn make_writer(&'w self) -> Self::Writer {
        self.clone()
    }
}

/// Install a permissive global `tracing` default once, so no callsite latches `Interest::never`
/// while another test of this binary runs with no subscriber in scope (the pattern
/// `custodian_day_one.rs` documents at its `enable_metric_callsites`) — and, in the same once,
/// register the callsites a dropped meter provider logs at ([`register_meter_drop_callsites`]).
fn enable_log_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
        register_meter_drop_callsites();
    });
}

/// Drop one meter provider on this thread, inside no `tracing` dispatch, before the caller builds
/// a role — so that the log lines the OpenTelemetry SDK writes when a provider is dropped have
/// their callsites registered here, and not under `tracing`'s dispatcher-list lock.
///
/// A role's dispatch owns a meter provider, and the SDK logs through `tracing` as the last
/// reference to one goes. `tracing` registers a callsite the first time it is hit, which takes
/// its dispatcher-list lock; and while it registers a dispatch or a callsite it holds that lock
/// with a temporary reference to every live dispatch (`tracing-core` 0.1.36, `callsite.rs`,
/// `register_dispatch` and `Rebuilder::for_each`). A test that drops its role at that moment
/// leaves the temporary reference the last one, so the provider is dropped under the lock, its
/// first log line goes to register its callsite, and that waits on the lock its own thread
/// holds: every test of the binary then blocks on that lock for good. It is a race between the
/// tests of one binary — the more of them build and drop a role as the binary starts, the
/// likelier — and it needs the provider's callsites to be unregistered still. Once they are
/// registered, a provider dropped under the lock takes no lock.
fn register_meter_drop_callsites() {
    drop(DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap());
}

/// Create's runtime here: one fixed identity, no jitter, no wait.
struct OneIdentity(Option<(UploadId, SegmentNonce)>);

impl CreateRuntime for OneIdentity {
    fn mint(&mut self) -> (UploadId, SegmentNonce) {
        self.0.take().expect("one identity per create")
    }

    fn jitter(&mut self) -> u64 {
        0
    }

    fn wait(&mut self, _millis: u64) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

/// The production Create of upload `n`, targeting `object`, under the shipped profile: the one
/// the deployed custodian holds as its own, so its multipart reaper's profile check admits the
/// pass whose retirement leg this file drives (a stored profile it is not sized for refuses the
/// whole leg, #625).
async fn create_upload(meta: &MemMeta, n: u64, object: &str) -> UploadId {
    let profile = Budget::shipped();
    let request = CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object,
        content_type: None,
        created_at_millis: 1_000,
        clock_source: "wall",
    };
    let identity = (
        UploadId::new(format!("{n:032x}")).unwrap(),
        SegmentNonce::new(format!("{:032x}", 0x5e60_0000 + n)).unwrap(),
    );
    let created = create_session(meta, &profile, &request, &mut OneIdentity(Some(identity)))
        .await
        .unwrap();
    let CreateOutcome::Created { upload_id } = created else {
        panic!("expected Created, got {created:?}");
    };
    upload_id
}

/// The owned staging entry of `chunk`, staged by part `part` of `id` at `placement` under
/// `scheme` — a placement whose length is not the scheme's fragment count is admitted, as the
/// staged placement admits it: its key and value.
fn owned_entry(
    id: &UploadId,
    part: u32,
    chunk: ChunkId,
    scheme: EcScheme,
    placement: Vec<DServerId>,
) -> (Vec<u8>, Bytes) {
    let staged = StagedPlacement::new(scheme, placement).unwrap();
    let entry = OwnedEntry::new(id.clone(), 20_000, staged).to_pending();
    let key = sidx_key(id, PartNumber::new(part).unwrap(), chunk);
    (key, metadata::encode(&entry))
}

/// The digest part `n` is committed and named under.
fn digest_of(n: u32) -> Digest {
    let mut bytes = [0x3c; 32];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    Digest::from_bytes(bytes)
}

/// The `part:` record of part `n` of `id` over `chunks`, checked to decode, and its `psum:`
/// summary.
fn part_records(id: &UploadId, n: u32, chunks: &[ChunkRef]) -> Vec<(Vec<u8>, Bytes)> {
    let digest = digest_of(n);
    let len: u64 = chunks.iter().map(|c| c.len).sum();
    let value = Bytes::from(format!(
        "{{\"chunks\":{},\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":1500,\"session_epoch\":0}}",
        String::from_utf8(metadata::encode(&chunks).to_vec()).unwrap()
    ));
    decode_part_record(&value).expect("a canonical part");
    let summary = PartSummary {
        chunks: chunks.len() as u32,
        len,
        digest,
        committed_at_millis: 1_500,
    };
    let number = PartNumber::new(n).unwrap();
    vec![
        (part_key(id, number), value),
        (psum_key(id, number), metadata::encode(&summary)),
    ]
}

/// Fragment `i` of `chunk` on server `i`, for every `i` of `servers`.
fn diagonal(chunk: ChunkId, servers: &[u16]) -> Vec<(DServerId, FragmentId)> {
    servers
        .iter()
        .map(|&i| (DServerId::from(i), FragmentId { chunk, index: i }))
        .collect()
}

/// Put a fragment at every position of `at` on the servers it names.
async fn land(servers: &[Arc<MemDServer>], at: &[(DServerId, FragmentId)]) {
    for &(dserver, frag) in at {
        let bytes = Bytes::from(format!("{frag:?}"));
        servers[dserver as usize]
            .put_fragment(frag, bytes, None)
            .await
            .unwrap();
    }
}

/// How many `orphan:` marks name a fragment of `chunk`.
async fn marks_of(meta: &MemMeta, chunk: ChunkId) -> usize {
    meta.scan(b"orphan:")
        .await
        .unwrap()
        .iter()
        .filter(|(key, _)| parse_orphan_key(key).is_some_and(|(_, frag)| frag.chunk == chunk))
        .count()
}

/// The admission ledger's count.
async fn sessions_counted(meta: &MemMeta) -> u64 {
    let ledger = meta.get(MPUCTL_KEY).await.unwrap().expect("the ledger");
    decode_admission_record(&ledger).unwrap().count()
}

/// Drive the REAL run loop for `passes` passes — counted by `probed`'s reachability probes, one a
/// pass — over `servers`, the operator having wired `operator_fleet_size` endpoints, with the
/// role built as `wyrd custodian` builds it (`with_logging_to`) and its log sink in `capture`.
/// The loop's clock reads [`LOOP_NOW`].
async fn drive_passes(
    meta: &MemMeta,
    servers: &[ConfiguredDServer],
    operator_fleet_size: usize,
    probed: &MemDServer,
    passes: u64,
    capture: &Capture,
) {
    drive_passes_at(
        meta,
        servers,
        operator_fleet_size,
        probed,
        passes,
        capture,
        LOOP_NOW,
    )
    .await;
}

/// [`drive_passes`], the loop's clock reading `now_millis` on every read.
async fn drive_passes_at(
    meta: &MemMeta,
    servers: &[ConfiguredDServer],
    operator_fleet_size: usize,
    probed: &MemDServer,
    passes: u64,
    capture: &Capture,
    now_millis: u64,
) {
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let log = wyrd_server::logging::LogConfig::new(Some("warn"), None).unwrap();
    let service = CustodianService::with_logging_to(telemetry, &log, capture.clone());
    let until = probed.probes() + passes;
    let shutdown = async move {
        while probed.probes() < until {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let run = service.run_reconstruction_until(
        &zone,
        &custodian,
        meta,
        servers,
        operator_fleet_size,
        ExpiredPendingPolicy::Defer,
        Duration::from_millis(1),
        move || now_millis,
        shutdown,
    );
    tokio::time::timeout(Duration::from_secs(120), run)
        .await
        .expect("the passes ran within the bound")
        .expect("the deployed run loop survives and exits cleanly at shutdown");
}

#[tokio::test]
async fn deployed_retirement_leg_waits_for_the_whole_operator_fleet_and_keeps_completed_tombstones()
{
    // The deployed loop hands the retirement leg two inputs of its own (`custodian.rs`,
    // `run_reconstruction_until`): `operator_fleet_size`, the count it gates GC on, since an
    // untrusted record of an aborted upload is retired by CHUNK ID from the fleet's listings and a
    // listing that misses a server strands that server's fragments; and the tombstone input,
    // `Retain` until #625 measures `W_tombstone`, since deleting a completed upload's record early
    // turns an identical Complete retry from `AlreadyCompleted` into `NoSuchUpload`.
    enable_log_callsites();
    let meta = MemMeta::default();
    meta.commit(WriteBatch::new().put(bucket_key(BUCKET), Bytes::from_static(BUCKET_MARKER)))
        .await
        .unwrap();
    let d: Vec<Arc<MemDServer>> = (0..6).map(|_| Arc::new(MemDServer::default())).collect();

    // An ABORTED upload: an untrusted staging entry — RS(4,2) placed on five servers — with its
    // chunk's six fragments on servers 0–5, and beside it a trusted one, RS(2,1) on servers 0–2.
    let aborted = create_upload(&meta, 1, &format!("{BUCKET}/aborted")).await;
    const UNTRUSTED: ChunkId = 0x0A11;
    const ABORTED_RESIDUE: ChunkId = 0x0A12;
    let untrusted = owned_entry(&aborted, 1, UNTRUSTED, RS42, vec![0, 1, 2, 3, 4]);
    let residue = owned_entry(&aborted, 2, ABORTED_RESIDUE, RS21, vec![0, 1, 2]);
    let untrusted_at = diagonal(UNTRUSTED, &[0, 1, 2, 3, 4, 5]);
    land(&d, &untrusted_at).await;
    land(&d, &diagonal(ABORTED_RESIDUE, &[0, 1, 2])).await;
    let mut batch = WriteBatch::new();
    batch.puts.extend([untrusted.clone(), residue.clone()]);
    meta.commit(batch).await.unwrap();
    let fenced = abort_session(&meta, &aborted).await;
    assert!(matches!(fenced, Ok(AbortOutcome::Fenced)), "{fenced:?}");

    // A COMPLETED upload: part 1 published, and the trusted residue of a crashed part 2.
    let object = format!("{BUCKET}/completed");
    let completed = create_upload(&meta, 2, &object).await;
    const PUBLISHED: ChunkId = 0x0C11;
    const COMPLETED_RESIDUE: ChunkId = 0x0C12;
    let published = ChunkRef {
        id: PUBLISHED,
        scheme: RS21,
        len: 64,
        placement: vec![0, 1, 2],
    };
    land(&d, &diagonal(PUBLISHED, &[0, 1, 2])).await;
    let completed_residue = owned_entry(&completed, 2, COMPLETED_RESIDUE, RS21, vec![0, 1, 2]);
    land(&d, &diagonal(COMPLETED_RESIDUE, &[0, 1, 2])).await;
    let mut batch = WriteBatch::new();
    batch.puts.extend(part_records(
        &completed,
        1,
        std::slice::from_ref(&published),
    ));
    batch.puts.push(completed_residue.clone());
    meta.commit(batch).await.unwrap();
    let named = NamedParts::new(vec![(PartNumber::new(1).unwrap(), digest_of(1))]).unwrap();
    let request = CompleteRequest {
        bucket: BUCKET,
        parent: ROOT,
        object: &object,
        parts: &named,
        fenced_at_millis: 2_000,
        clock_source: "wall",
    };
    let publish = PublishRequest {
        fresh_inode: 9,
        completed_at_millis: 3_000,
    };
    let outcome = complete_upload(&meta, &completed, &request, &publish).await;
    assert!(
        matches!(outcome, Ok(CompleteOutcome::Published(_))),
        "{outcome:?}"
    );
    let counted = sessions_counted(&meta).await;
    assert_eq!(counted, 2);

    let build = |ids: std::ops::Range<u16>| -> Vec<ConfiguredDServer> {
        ids.map(|i| ConfiguredDServer {
            id: DServerId::from(i),
            failure_domain: format!("domain-{i}"),
            store: dyn_store(&d[usize::from(i)]),
        })
        .collect()
    };

    // The operator wired SIX endpoints; server 5 — which holds fragment 5 of the untrusted chunk —
    // was unreachable at startup, so `connect_fleet` dropped it and the loop holds five. Every one
    // of them is reachable, so nothing is `unreachable`: only the operator count says the fleet is
    // short.
    const OPERATOR_FLEET_SIZE: usize = 6;
    let capture = Capture::default();
    drive_passes(&meta, &build(0..5), OPERATOR_FLEET_SIZE, &d[0], 6, &capture).await;

    assert_eq!(
        meta.get(&untrusted.0).await.unwrap(),
        Some(untrusted.1.clone()),
        "the untrusted entry is held on the startup-partial fleet, byte-identical (RED if the loop \
         hands the leg its live fleet's length: the entry is retired on a listing that misses \
         server 5)"
    );
    assert_eq!(
        marks_of(&meta, UNTRUSTED).await,
        0,
        "nothing of its chunk is marked"
    );
    for &(dserver, frag) in &untrusted_at {
        assert!(
            present(&d[dserver as usize], frag.chunk, frag.index).await,
            "{frag:?} on {dserver} stays"
        );
    }
    let log = capture.contents();
    assert!(
        log.lines().any(|line| line.contains("retire-session-held")
            && line.contains("partial-fleet")
            && line.contains(std::str::from_utf8(&untrusted.0).unwrap())),
        "the hold reaches the log sink, typed `partial-fleet` and naming the entry: {log}"
    );
    assert!(meta.get(&mpu_key(&aborted)).await.unwrap().is_some());
    // Not vacuous: the leg ran over both uploads on this fleet, walking their trusted residue away.
    for (key, chunk) in [
        (&residue.0, ABORTED_RESIDUE),
        (&completed_residue.0, COMPLETED_RESIDUE),
    ] {
        assert_eq!(meta.get(key).await.unwrap(), None, "the residue is walked");
        assert_eq!(marks_of(&meta, chunk).await, 3, "every position marked");
    }
    assert!(
        meta.get(&mpu_key(&completed)).await.unwrap().is_some(),
        "the completed upload's record — its tombstone — stays (RED if the loop hands the leg \
         `Elapsed`)"
    );
    assert_eq!(sessions_counted(&meta).await, counted);

    // Server 5 is back: the operator now sees all six. The untrusted entry retires by chunk id, the
    // aborted upload ends and releases its slot; the completed upload's record still stays.
    drive_passes(&meta, &build(0..6), OPERATOR_FLEET_SIZE, &d[0], 6, &capture).await;

    assert_eq!(meta.get(&untrusted.0).await.unwrap(), None, "retired");
    assert_eq!(
        marks_of(&meta, UNTRUSTED).await,
        6,
        "every fragment of its chunk marked, server 5's included"
    );
    assert_eq!(meta.get(&mpu_key(&aborted)).await.unwrap(), None, "ended");
    assert!(
        meta.get(&mpu_key(&completed)).await.unwrap().is_some(),
        "the tombstone still stays"
    );
    assert_eq!(sessions_counted(&meta).await, counted - 1);
}

// ---- the loop's reconstruction pass carries the multipart REAPER ----

#[tokio::test]
async fn deployed_loop_reaps_an_idle_open_upload_and_tears_it_down() {
    // The deployed loop is the reaper's production caller (`custodian.rs`,
    // `run_reconstruction_until`): every interval its reconstruction pass hands `reconcile_step`
    // a `ReaperContext`, so an `Open` upload whose client walked away is fenced by the reaper and
    // drained by the same step's retirement leg. Nothing else in the loop ends an `Open` upload,
    // so with no reaper passed this one keeps its records and its admission slot for good.
    enable_log_callsites();
    let meta = MemMeta::default();
    meta.commit(WriteBatch::new().put(bucket_key(BUCKET), Bytes::from_static(BUCKET_MARKER)))
        .await
        .unwrap();
    let d: Vec<Arc<MemDServer>> = (0..3).map(|_| Arc::new(MemDServer::default())).collect();
    let servers: Vec<ConfiguredDServer> = (0..3u16)
        .map(|i| ConfiguredDServer {
            id: DServerId::from(i),
            failure_domain: format!("domain-{i}"),
            store: dyn_store(&d[usize::from(i)]),
        })
        .collect();

    // An upload created at 1 000 ms on the wall clock and never aborted or completed: part 1
    // committed at 1 500 ms, and the staging entry of a crashed part 2 whose lease ran out at
    // 20 000 ms. No client touches it again.
    let idle = create_upload(&meta, 3, &format!("{BUCKET}/idle")).await;
    const COMMITTED: ChunkId = 0x1D11;
    const RESIDUE: ChunkId = 0x1D12;
    let committed = ChunkRef {
        id: COMMITTED,
        scheme: RS21,
        len: 64,
        placement: vec![0, 1, 2],
    };
    let part = part_records(&idle, 1, std::slice::from_ref(&committed));
    let residue = owned_entry(&idle, 2, RESIDUE, RS21, vec![0, 1, 2]);
    land(&d, &diagonal(COMMITTED, &[0, 1, 2])).await;
    land(&d, &diagonal(RESIDUE, &[0, 1, 2])).await;
    let mut batch = WriteBatch::new();
    batch.puts.extend(part.iter().cloned());
    batch.puts.push(residue.clone());
    meta.commit(batch).await.unwrap();
    let session = meta.get(&mpu_key(&idle)).await.unwrap().expect("created");
    assert_eq!(sessions_counted(&meta).await, 1);
    let capture = Capture::default();

    // Hours old: inside the idle window, so the loop's passes leave every record of it alone.
    drive_passes(&meta, &servers, servers.len(), &d[0], 3, &capture).await;

    assert_eq!(
        meta.get(&mpu_key(&idle)).await.unwrap(),
        Some(session),
        "an upload that is not idle yet is left Open, byte-identical"
    );
    for (key, value) in part.iter().chain([&residue]) {
        assert_eq!(meta.get(key).await.unwrap().as_ref(), Some(value));
    }
    assert_eq!(marks_of(&meta, COMMITTED).await, 0);
    assert_eq!(marks_of(&meta, RESIDUE).await, 0);
    assert_eq!(sessions_counted(&meta).await, 1);

    // Eight days on: past the idle window (seven days) with every lease long lapsed, and well
    // inside the residency ceiling (thirty), so only the reaper's idle arm can end it.
    const EIGHT_DAYS: u64 = 8 * 24 * 60 * 60 * 1_000;
    drive_passes_at(
        &meta,
        &servers,
        servers.len(),
        &d[0],
        6,
        &capture,
        EIGHT_DAYS,
    )
    .await;

    assert_eq!(
        meta.get(&mpu_key(&idle)).await.unwrap(),
        None,
        "the idle upload is fenced and its teardown finished (RED if the loop passes no reaper: \
         nothing else ends an Open upload)"
    );
    for prefix in ["part:", "psum:", "sidx:", "slot:", "retire:"] {
        let left = meta.scan(prefix.as_bytes()).await.unwrap();
        assert!(left.is_empty(), "{prefix} is drained: {left:?}");
    }
    // Every position of both byte sets is marked, each by name, and nothing else of either chunk:
    // the committed part's through the obligation only a fence installs — no client abort ran, so
    // the fence was the reaper's — and the crashed part's residue through the owned walk.
    for chunk in [COMMITTED, RESIDUE] {
        for (dserver, frag) in diagonal(chunk, &[0, 1, 2]) {
            assert!(
                orphan_record_present(&meta, dserver, frag.chunk, frag.index).await,
                "{frag:?} on server {dserver} is marked"
            );
        }
        assert_eq!(
            marks_of(&meta, chunk).await,
            3,
            "no other mark of {chunk:#x}"
        );
    }
    assert_eq!(
        sessions_counted(&meta).await,
        0,
        "its admission slot is released"
    );
}

// ---- the multipart reaper runs on a DEGRADED fleet, outside GC's whole-fleet gate ----

/// Leg A's fixture from `multipart_reaper.rs`, seeded raw at the loop clock `now` (#625, slice
/// 625.2): an idle `Open` session created 14 days before `now`, its progress stamped 10 days
/// before, every lease lapsed 9 days before; one committed `part:`/`psum:` pair over `committed`;
/// the slot and owned `sidx:` entry of a crashed part over `in_flight`; `mpuctl.count = 1`. The
/// record spellings are copied from that file, not shared.
fn leg_a_records(
    id: &UploadId,
    now: u64,
    committed: &ChunkRef,
    in_flight: &ChunkRef,
) -> Vec<(Vec<u8>, Bytes)> {
    use wyrd_core::multipart as mp;
    const DAY: u64 = 24 * 60 * 60 * 1_000;
    fn canonical<T: serde::Serialize + serde::de::DeserializeOwned>(json: &str) -> Bytes {
        let value: T = metadata::decode(json.as_bytes()).expect("a legal record");
        metadata::encode(&value)
    }
    let profile = format!(
        "{{\"w_ref\":{},\"max_part_chunks\":{},\"max_parts_per_session\":{},\
         \"max_inflight_parts\":{},\"max_staged_chunks\":{}}}",
        mp::W_REF,
        mp::MAX_PART_CHUNKS,
        mp::MAX_PARTS_PER_SESSION,
        mp::MAX_INFLIGHT_PARTS,
        mp::MAX_STAGED_CHUNKS
    );
    let budget: Budget = metadata::decode(profile.as_bytes()).expect("a legal profile");
    let ledger = format!(
        "{{\"count\":1,\"max_sessions\":{},\"profile\":{profile}}}",
        budget.max_sessions()
    );
    let session = format!(
        "{{\"parent\":42,\"object\":\"bucket/object-{id}\",\"created_at_millis\":{},\
         \"clock_source\":\"wall\",\"segment_nonce\":\"{}\",\"epoch\":0,\"attempts\":0,\
         \"state\":{{\"kind\":\"Open\"}}}}",
        now - 14 * DAY,
        id.as_str()
    );
    let slot = format!(
        "{{\"part_number\":2,\"attempt_id\":\"{}\",\"reserved_at_millis\":{},\
         \"lease_expiry_millis\":{}}}",
        "ab".repeat(16),
        now - 10 * DAY,
        now - 9 * DAY
    );
    let staged = StagedPlacement::new(in_flight.scheme, in_flight.placement.clone()).unwrap();
    let owned = OwnedEntry::new(id.clone(), now - 9 * DAY, staged).to_pending();
    let digest = format!("{:064x}", 1);
    let chunks_json =
        String::from_utf8(metadata::encode(&std::slice::from_ref(committed)).to_vec()).unwrap();
    let at = now - 10 * DAY;
    let part = format!(
        "{{\"chunks\":{chunks_json},\"len\":{},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{at},\"session_epoch\":0}}",
        committed.len
    );
    let summary = format!(
        "{{\"chunks\":1,\"len\":{},\"digest\":\"{digest}\",\"committed_at_millis\":{at}}}",
        committed.len
    );
    let one = PartNumber::new(1).unwrap();
    vec![
        (
            MPUCTL_KEY.to_vec(),
            canonical::<mp::AdmissionRecord>(&ledger),
        ),
        (mpu_key(id), canonical::<mp::SessionRecord>(&session)),
        (
            mp::slot_key(id, mp::SlotIndex::new(0).unwrap()),
            canonical::<mp::SlotRecord>(&slot),
        ),
        (
            sidx_key(id, PartNumber::new(2).unwrap(), in_flight.id),
            metadata::encode(&owned),
        ),
        (part_key(id, one), canonical::<mp::PartRecord>(&part)),
        (psum_key(id, one), canonical::<PartSummary>(&summary)),
    ]
}

/// Leg A's upload and its two byte sets (#625, slice 625.2), as [`leg_a_records`] spells them.
struct LegA {
    id: UploadId,
    committed: ChunkRef,
    in_flight: ChunkRef,
}

impl LegA {
    fn new() -> Self {
        Self {
            id: UploadId::new("a1".repeat(16)).unwrap(),
            committed: ChunkRef {
                id: 0xa1_0001,
                scheme: RS21,
                len: 4_096,
                placement: vec![1, 2, 3],
            },
            in_flight: ChunkRef {
                id: 0xa1_0002,
                scheme: RS21,
                len: 4_096,
                placement: vec![4, 5, 6],
            },
        }
    }

    /// Seed `meta` with leg A at the loop clock `now`.
    async fn seed(&self, meta: &MemMeta, now: u64) {
        let mut batch = WriteBatch::new();
        batch.puts.extend(leg_a_records(
            &self.id,
            now,
            &self.committed,
            &self.in_flight,
        ));
        meta.commit(batch).await.unwrap();
        assert_eq!(sessions_counted(meta).await, 1);
    }

    /// Assert leg A's transition on `meta`: every record of the upload gone, both of its
    /// `retire:` ranges empty, its admission slot released, and every position of both byte sets
    /// marked. `how` names the run, for the message.
    async fn assert_torn_down(&self, meta: &MemMeta, how: &str) {
        let id = &self.id;
        assert_eq!(
            meta.get(&mpu_key(id)).await.unwrap(),
            None,
            "{how}: the idle upload is fenced and its teardown finished"
        );
        for prefix in [
            wyrd_core::multipart::part_range(id),
            wyrd_core::multipart::psum_range(id),
            wyrd_core::multipart::sidx_range(id),
            wyrd_core::multipart::slot_range(id),
            wyrd_core::multipart::retire_session_range(wyrd_core::multipart::RetireMode::Bytes, id),
            wyrd_core::multipart::retire_session_range(
                wyrd_core::multipart::RetireMode::Records,
                id,
            ),
        ] {
            let left = meta.scan(&prefix).await.unwrap();
            assert!(left.is_empty(), "{how}: drained: {left:?}");
        }
        assert_eq!(
            sessions_counted(meta).await,
            0,
            "{how}: its admission slot is released"
        );
        for chunk in [&self.committed, &self.in_flight] {
            for (index, &dserver) in chunk.placement.iter().enumerate() {
                assert!(
                    orphan_record_present(meta, dserver, chunk.id, index as u16).await,
                    "{how}: fragment {index} of {:#x} on server {dserver} is marked",
                    chunk.id
                );
            }
            assert_eq!(marks_of(meta, chunk.id).await, 3, "{how}");
        }
    }
}

#[tokio::test]
async fn deployed_loop_reaps_an_idle_upload_on_a_degraded_fleet_while_gc_is_deferred() {
    // The operator wired four D servers; one was down at startup, so `connect_fleet` handed the
    // loop three. GC's whole-fleet gate (`fleet.len() == operator_fleet_size`) defers GC on every
    // pass — and the multipart reaper must not wait with it: it runs on whatever part of the fleet
    // is reachable (#625, 0016 `:234-268`). Leg A's idle upload is fenced and torn down with every
    // byte evidenced.
    enable_log_callsites();
    const OPERATOR_FLEET_SIZE: usize = 4;
    let meta = MemMeta::default();
    let d: Vec<Arc<MemDServer>> = (0..3).map(|_| Arc::new(MemDServer::default())).collect();
    let servers: Vec<ConfiguredDServer> = (0..3u16)
        .map(|i| ConfiguredDServer {
            id: DServerId::from(i),
            failure_domain: format!("domain-{i}"),
            store: dyn_store(&d[usize::from(i)]),
        })
        .collect();
    assert!(
        servers.len() < OPERATOR_FLEET_SIZE,
        "a degraded fleet: GC is deferred on every pass"
    );
    let leg = LegA::new();
    leg.seed(&meta, LEG_A_NOW).await;

    let capture = Capture::default();
    drive_passes_at(
        &meta,
        &servers,
        OPERATOR_FLEET_SIZE,
        &d[0],
        6,
        &capture,
        LEG_A_NOW,
    )
    .await;

    leg.assert_torn_down(
        &meta,
        "on the degraded fleet (RED if the reaper is dispatched only inside GC's whole-fleet \
         branch)",
    )
    .await;
}

// ---- the role's one interval driver: its stop, its retries, its fence (#625, slice 625.2) ----

/// The loop clock leg A is seeded and judged at: far enough past the epoch for its 14-day-old
/// stamps.
const LEG_A_NOW: u64 = 40 * 24 * 60 * 60 * 1_000;

/// How many seeds the no-fleet loop's fault run is replayed under.
const FAULT_SEEDS: u64 = 32;

/// How many of its first store calls a seeded run may fail. Leg A's clean teardown takes three
/// passes; this window covers the first of them and more, and every call after it is answered,
/// so a loop that retries converges.
const FAULT_WINDOW: u64 = 40;

/// Each call inside [`FAULT_WINDOW`] fails with a one-in-this chance, drawn from the seed.
const FAULT_ONE_IN: u32 = 5;

/// The operator signal the reaper raises for a fence whose commit came back with an outcome the
/// store could not report (`wyrd_custodian` reaper, `fence_unknown`).
const FENCE_OUTCOME_UNKNOWN: &str = "reaper-fence-outcome-unknown";

/// A metadata store over [`MemMeta`] that fails a seeded share of its first calls: each of the
/// first [`FAULT_WINDOW`] fails with a one-in-[`FAULT_ONE_IN`] chance drawn from the run's
/// [`Sim`], and one of them, drawn too, fails for certain; every later call is answered. A failed
/// read answers an error and nothing else. A failed commit is applied or not — drawn as well — and
/// answers the seam's own [`CommitUnknownResult`] either way, as a networked backend answers a
/// commit it could not confirm: never a plain error, which the contract reads as "nothing was
/// written" and which would be false of every applied draw. `may_still_commit` is `false`: the
/// batch is in or out before the error is returned, so one re-read settles it. Both draws are
/// counted, and so are the calls that came before `armed` was set.
struct FlakyMeta<'a> {
    inner: &'a MemMeta,
    sim: Mutex<Sim>,
    certain: u64,
    armed: &'a AtomicBool,
    calls: AtomicU64,
    faults: AtomicU64,
    unknown_applied: AtomicU64,
    unknown_unapplied: AtomicU64,
    unarmed_calls: AtomicU64,
}

impl<'a> FlakyMeta<'a> {
    fn new(inner: &'a MemMeta, seed: u64, armed: &'a AtomicBool) -> Self {
        let mut sim = Sim::new(seed);
        let certain = sim.gen::<u64>() % FAULT_WINDOW;
        Self {
            inner,
            sim: Mutex::new(sim),
            certain,
            armed,
            calls: AtomicU64::new(0),
            faults: AtomicU64::new(0),
            unknown_applied: AtomicU64::new(0),
            unknown_unapplied: AtomicU64::new(0),
            unarmed_calls: AtomicU64::new(0),
        }
    }

    /// Whether this call fails.
    fn fails(&self) -> bool {
        if !self.armed.load(Ordering::SeqCst) {
            self.unarmed_calls.fetch_add(1, Ordering::SeqCst);
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call >= FAULT_WINDOW {
            return false;
        }
        let drawn = self
            .sim
            .lock()
            .unwrap()
            .gen::<u32>()
            .is_multiple_of(FAULT_ONE_IN);
        let fails = drawn || call == self.certain;
        if fails {
            self.faults.fetch_add(1, Ordering::SeqCst);
        }
        fails
    }

    fn fault<T>() -> Result<T> {
        Err("injected metadata fault".into())
    }
}

#[async_trait]
impl MetadataStore for FlakyMeta<'_> {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.fails() {
            return Self::fault();
        }
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if self.fails() {
            return Self::fault();
        }
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        if self.fails() {
            return Self::fault();
        }
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        if self.fails() {
            // Drawn: the batch is sent on and its answer lost, or it is never sent. A batch sent
            // on whose precondition fails writes nothing, so it counts as not applied.
            let sent = self.sim.lock().unwrap().gen::<bool>();
            let applied = sent && self.inner.commit(batch).await? == CommitOutcome::Committed;
            let drawn = if applied {
                &self.unknown_applied
            } else {
                &self.unknown_unapplied
            };
            drawn.fetch_add(1, Ordering::SeqCst);
            return Err(Box::new(CommitUnknownResult {
                backend: "flaky-mem",
                code: None,
                detail: "injected fault: the answer to this commit was lost".to_owned(),
                // In or out already: nothing is in flight once this error is returned.
                may_still_commit: false,
            }));
        }
        self.inner.commit(batch).await
    }
}

/// **The no-fleet loop rides out seeded store faults** and finishes leg A's teardown (#625, slice
/// 625.2). The loop the role with no `--endpoints` runs ([`CustodianService::run_reaper_until`])
/// is driven over [`FlakyMeta`] under each of [`FAULT_SEEDS`] seeds: reads, listings and commits
/// fail at seeded calls of its first passes, a failed commit applied or not and answered with
/// [`CommitUnknownResult`] either way. A pass a fault fails is logged and the next interval runs
/// another, so the loop must still be running when leg A's session record is gone — its stop is
/// that — and return `Ok`, with leg A's whole transition on the store. Each seed also checks that
/// the loop polled its stop before its first store call: the stop here, like the role's Ctrl-C,
/// registers itself on its first poll.
///
/// Over the seeds both draws of a failed commit must have happened — applied and not applied —
/// and the reaper must have taken at least one of them on its fence, down the path it keeps for a
/// commit whose outcome the store could not report (its `reaper-fence-outcome-unknown` signal):
/// the seeds converge through that path, not around it.
#[tokio::test]
async fn no_fleet_loop_rides_out_seeded_store_faults_to_leg_a_teardown() {
    enable_log_callsites();
    // One role for every seed, built as `wyrd custodian` builds it, its log sink captured: the
    // reaper names a fence whose outcome it could not learn there.
    let capture = Capture::default();
    let log = wyrd_server::logging::LogConfig::new(Some("warn"), None).unwrap();
    let service = CustodianService::with_logging_to(
        DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap(),
        &log,
        capture.clone(),
    );
    let mut unknown_applied = 0;
    let mut unknown_unapplied = 0;
    for seed in 0..FAULT_SEEDS {
        let meta = MemMeta::default();
        let leg = LegA::new();
        leg.seed(&meta, LEG_A_NOW).await;
        let armed = AtomicBool::new(false);
        let flaky = FlakyMeta::new(&meta, seed, &armed);
        let coord = MemCoordination::new();
        let (zone, custodian) = elect(&coord).await;
        let session = mpu_key(&leg.id);
        let shutdown = async {
            // Registered on the first poll, as `ctrl_c()` installs its handler.
            armed.store(true, Ordering::SeqCst);
            while meta.get(&session).await.unwrap().is_some() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let run = service.run_reaper_until(
            &zone,
            &custodian,
            &flaky,
            Duration::from_millis(1),
            || LEG_A_NOW,
            shutdown,
        );
        tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .unwrap_or_else(|_| panic!("seed {seed}: leg A's teardown did not converge"))
            .unwrap_or_else(|e| panic!("seed {seed}: a store fault ended the loop: {e}"));

        assert_eq!(
            flaky.unarmed_calls.load(Ordering::SeqCst),
            0,
            "seed {seed}: the loop called the store before it polled its stop"
        );
        assert!(
            flaky.faults.load(Ordering::SeqCst) >= 1,
            "seed {seed}: faults were injected"
        );
        leg.assert_torn_down(&meta, &format!("seed {seed}")).await;
        unknown_applied += flaky.unknown_applied.load(Ordering::SeqCst);
        unknown_unapplied += flaky.unknown_unapplied.load(Ordering::SeqCst);
    }
    let unknown_fences = capture.contents().matches(FENCE_OUTCOME_UNKNOWN).count();
    assert!(
        unknown_applied >= 1 && unknown_unapplied >= 1,
        "over {FAULT_SEEDS} seeds a failed commit was drawn both ways: {unknown_applied} applied, \
         {unknown_unapplied} not applied"
    );
    assert!(
        unknown_fences >= 1,
        "over {FAULT_SEEDS} seeds the reaper took a failed fence commit as an outcome it could \
         not learn (`{FENCE_OUTCOME_UNKNOWN}`) at least once"
    );
}

/// **The no-fleet loop stops when a newer term holds the zone**, having changed nothing (#625,
/// slice 625.2). A deposed custodian's [`CustodianService::run_reaper_until`] returns
/// `Err(Fenced)` from its first pass — a superseded leader must not keep acting — and leg A's
/// store is exactly as seeded.
#[tokio::test]
async fn no_fleet_loop_stops_when_a_newer_term_holds_the_zone() {
    enable_log_callsites();
    let meta = MemMeta::default();
    LegA::new().seed(&meta, LEG_A_NOW).await;
    let seeded = meta.kv.lock().unwrap().clone();

    let coord = MemCoordination::new();
    let deposed = Custodian::elect(&coord, "zone-reconstruction")
        .await
        .unwrap();
    let mut zone = FencedZone::new();
    zone.install(deposed.leadership());
    let newer = Custodian::elect(&coord, "zone-reconstruction")
        .await
        .unwrap();
    zone.install(newer.leadership());
    assert!(newer.term() > deposed.term());
    let service =
        CustodianService::new(DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap());

    let run = service.run_reaper_until(
        &zone,
        &deposed,
        &meta,
        Duration::from_millis(1),
        || LEG_A_NOW,
        std::future::pending::<()>(),
    );
    let err = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("a fenced loop stops by itself")
        .expect_err("a deposed custodian's no-fleet loop stops rather than act");
    assert!(
        matches!(err, ReconcileError::Fenced(_)),
        "fenced, not a store fault: {err}"
    );
    assert!(
        *meta.kv.lock().unwrap() == seeded,
        "the deposed custodian changed the store"
    );
}

/// **The fleet loop polls its stop before its first pass, and a stop requested by then ends the
/// loop after that pass** (#625, slice 625.2). The role's Ctrl-C registers itself only when first
/// polled; polled after the first pass, a Ctrl-C during that pass killed the process. The stop
/// here records how many reachability probes — one a pass — the fleet had answered when it was
/// first polled, and is done at once: it must have been polled before any probe, and the loop must
/// then run exactly one pass and return `Ok`.
#[tokio::test]
async fn fleet_loop_polls_its_stop_before_its_first_pass() {
    enable_log_callsites();
    let meta = MemMeta::default();
    let d: Vec<Arc<MemDServer>> = (0..3).map(|_| Arc::new(MemDServer::default())).collect();
    let servers: Vec<ConfiguredDServer> = (0..3u16)
        .map(|i| ConfiguredDServer {
            id: DServerId::from(i),
            failure_domain: format!("domain-{i}"),
            store: dyn_store(&d[usize::from(i)]),
        })
        .collect();
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let service =
        CustodianService::new(DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap());
    let probes_when_polled = AtomicU64::new(u64::MAX);
    let shutdown = async {
        probes_when_polled.store(d[0].probes(), Ordering::SeqCst);
    };

    let run = service.run_reconstruction_until(
        &zone,
        &custodian,
        &meta,
        &servers,
        servers.len(),
        ExpiredPendingPolicy::Defer,
        Duration::from_millis(1),
        || LEG_A_NOW,
        shutdown,
    );
    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the loop stops by itself")
        .expect("and cleanly");

    assert_eq!(
        probes_when_polled.load(Ordering::SeqCst),
        0,
        "the stop is polled before the first pass probes the fleet"
    );
    assert_eq!(
        d[0].probes(),
        1,
        "a stop requested before the first pass ends the loop after it, not before"
    );
}

/// The tally every reaper pass emits once, at its end (`wyrd_custodian` reaper, `emit_pass`).
const REAPER_PASS_TALLY: &str = "monotonic_counter.reaper_passes";

/// How many times the both-ready wait is replayed for each shape of the loop. A wait that picks
/// between two ready branches at random starts no further pass in about half its rounds, so one
/// round proves nothing; this many all passing by that chance is one in 2^64.
const BOTH_READY_ROUNDS: usize = 64;

/// **A stop that is ready when the interval's timer is ready wins: the loop starts no further
/// pass** (#625, slice 625.2). Between two passes the loop waits on its stop and on its
/// interval's timer together, and a Ctrl-C can land as the interval runs out, so that one poll
/// finds both ready. Here that happens every round, on a paused clock: the stop is itself a timer
/// of the loop's own interval, started before the loop is. A paused clock does not move while the
/// first pass runs, so the timer the loop then starts for its wait falls due at the stop's own
/// instant, and the clock's one jump to that instant wakes the wait with both ready.
///
/// The stop records the clock at each of its polls, and the three it must show say the fixture
/// held: before the first pass and again as the wait began, the clock unmoved — so the two timers
/// have one deadline — then once more at that deadline. Each round, the loop with no fleet and
/// the loop over one must each run exactly one pass and return `Ok`.
#[tokio::test(start_paused = true)]
async fn a_stop_ready_with_the_interval_timer_starts_no_further_pass() {
    enable_log_callsites();
    const INTERVAL: Duration = Duration::from_secs(1);
    const BOUND: Duration = Duration::from_secs(60);
    // One role for every round, its log sink captured: each reaper pass writes its tally there.
    let capture = Capture::default();
    let log = wyrd_server::logging::LogConfig::new(Some("info"), None).unwrap();
    let service = CustodianService::with_logging_to(
        DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap(),
        &log,
        capture.clone(),
    );
    let mut tallied = 0;
    for round in 0..BOTH_READY_ROUNDS {
        for fleet in [false, true] {
            let shape = if fleet {
                "the fleet loop"
            } else {
                "the no-fleet loop"
            };
            let meta = MemMeta::default();
            // Leg A's idle upload: a pass that starts has an upload to act on.
            LegA::new().seed(&meta, LEG_A_NOW).await;
            let d: Vec<Arc<MemDServer>> = (0..3).map(|_| Arc::new(MemDServer::default())).collect();
            let servers: Vec<ConfiguredDServer> = (0..3u16)
                .map(|i| ConfiguredDServer {
                    id: DServerId::from(i),
                    failure_domain: format!("domain-{i}"),
                    store: dyn_store(&d[usize::from(i)]),
                })
                .collect();
            let coord = MemCoordination::new();
            let (zone, custodian) = elect(&coord).await;

            let started = tokio::time::Instant::now();
            let due = tokio::time::sleep(INTERVAL);
            tokio::pin!(due);
            let polled_at = Mutex::new(Vec::new());
            let shutdown = std::future::poll_fn(|cx| {
                polled_at.lock().unwrap().push(tokio::time::Instant::now());
                due.as_mut().poll(cx)
            });
            let ran = if fleet {
                let run = service.run_reconstruction_until(
                    &zone,
                    &custodian,
                    &meta,
                    &servers,
                    servers.len(),
                    ExpiredPendingPolicy::Defer,
                    INTERVAL,
                    || LEG_A_NOW,
                    shutdown,
                );
                tokio::time::timeout(BOUND, run).await
            } else {
                let run = service.run_reaper_until(
                    &zone,
                    &custodian,
                    &meta,
                    INTERVAL,
                    || LEG_A_NOW,
                    shutdown,
                );
                tokio::time::timeout(BOUND, run).await
            };
            ran.unwrap_or_else(|_| panic!("{shape}, round {round}: the loop did not stop"))
                .unwrap_or_else(|e| panic!("{shape}, round {round}: the loop failed: {e}"));

            assert_eq!(
                polled_at.into_inner().unwrap(),
                [started, started, started + INTERVAL],
                "{shape}, round {round}: the stop is polled before the first pass, again as the \
                 wait begins with the clock unmoved — the wait's timer and the stop then have one \
                 deadline — and once more at that deadline, both ready"
            );
            let tallies = capture.contents().matches(REAPER_PASS_TALLY).count();
            let passes = tallies - tallied;
            tallied = tallies;
            assert_eq!(
                passes, 1,
                "{shape}, round {round}: the stop was ready with the interval's timer, and the \
                 loop started another pass after it"
            );
            if fleet {
                assert_eq!(
                    d[0].probes(),
                    1,
                    "{shape}, round {round}: one pass probed the fleet, and no other began"
                );
            }
        }
    }
}
