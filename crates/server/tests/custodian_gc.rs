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
//! * [`deployed_run_loop_refuses_duplicate_endpoints`] /
//!   [`deployed_run_loop_refuses_duplicate_ids`] — the #554 ITERATION-4 correction: the fleet-
//!   identity uniqueness refusal used to guard ONLY the `--reconcile-after-restore` one-shot, but
//!   this bundle arms the DEPLOYED run-loop path with a DELETING GC pass. A duplicated endpoint (or
//!   two ids naming one box) fuses a server under two identities, so a live fragment protected as
//!   `(A, frag)` is unreferenced as `(B, frag)` and GC would DELETE it. Both tests drive the REAL
//!   `cli::cmd_custodian` entry on the run-loop path (no `--reconcile-after-restore`) and assert it
//!   REFUSES before dialing. RED on base (the run-loop path had no refusal, so the fused fleet
//!   reached connect_fleet); GREEN with the hoisted refusal.
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, orphan_key, pending_key, EcScheme, InodeId, PendingEntry};
use wyrd_core::placement::Topology;
use wyrd_core::write::write_new_object_placed;
use wyrd_custodian::{Custodian, ExpiredPendingPolicy, FencedZone};
use wyrd_server::custodian::{ConfiguredDServer, CustodianService};
use wyrd_telemetry::{DurabilityTelemetry, ExporterConfig};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
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
/// drops a server whose `health()` errs).
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
    healthy: AtomicBool,
}

impl Default for MemDServer {
    fn default() -> Self {
        Self {
            frags: Mutex::new(HashMap::new()),
            healthy: AtomicBool::new(true),
        }
    }
}

impl MemDServer {
    /// Take this server reachable / unreachable for the reachability probe (`health()`).
    fn set_reachable(&self, reachable: bool) {
        self.healthy.store(reachable, Ordering::SeqCst);
    }
}

#[async_trait]
impl ChunkStore for MemDServer {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
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
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        if let Some(store) = self.store(DServerId::from(id.index)) {
            store.put_fragment(id, fragment).await?;
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
    ) -> Result<()> {
        if let Some(store) = self.store(dserver) {
            store.put_fragment(id, fragment).await?;
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
    )
    .await
    .unwrap();
    d1.put_fragment(
        FragmentId {
            chunk: LEASED_CHUNK,
            index: 1,
        },
        Bytes::from_static(b"leased fan-out fragment 1"),
    )
    .await
    .unwrap();
    // Record the pending lease exactly as the write path's ledger does.
    meta.commit(WriteBatch::new().put(
        pending_key(LEASED_CHUNK),
        metadata::encode(&PendingEntry {
            lease_expiry_millis: LEASE_EXPIRY,
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
    )
    .await
    .unwrap();
    d1.put_fragment(
        FragmentId {
            chunk: LEASED_CHUNK,
            index: 1,
        },
        Bytes::from_static(b"in-flight fan-out fragment 1"),
    )
    .await
    .unwrap();
    meta.commit(WriteBatch::new().put(
        pending_key(LEASED_CHUNK),
        metadata::encode(&PendingEntry {
            lease_expiry_millis: LOGICAL_ZERO_LEASE_EXPIRY,
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
        )
        .await
        .unwrap();
    }
    meta.commit(WriteBatch::new().put(
        pending_key(LEASED_CHUNK),
        metadata::encode(&PendingEntry {
            lease_expiry_millis: LEASE_EXPIRY,
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
