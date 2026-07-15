//! The **deployable `wyrd custodian` role** — the runnable process that turns the M3
//! library maintenance plane into a running durability signal (observability floor,
//! proposal 0010 §"Scope boundary" item 2; the keystone the day-one signal rides on).
//!
//! M3 shipped the custodian's loops (`wyrd_custodian::reconcile_step`,
//! `reconstruction`, …) and, extracted at M4, the backend-agnostic telemetry seam
//! ([`wyrd_telemetry::DurabilityTelemetry`]) as **libraries**. Nothing bound them into a
//! deployable process: the custodian was a `dst`-only dependency, the server ran no
//! custodian loop (`cli.rs` "The CLI runs no custodian sweep"), and the durability
//! metrics a pass emits reached no export surface unless a caller wired one by hand. That
//! is the sim-only gap 0010 names — the day-one signal ("kill a D server → the
//! under-replicated count rises, then returns to zero") was only ever observed through a
//! bespoke test capture, never the real Prometheus/OTLP surface a deployment scrapes.
//!
//! [`CustodianService`] is the wiring that closes it, in the one crate that may know
//! concretes (ADR-0010): it **owns** the shared telemetry handle (proposal 0010 item 1,
//! installed at role entry) and runs the fenced [`reconcile_step`] control point with
//! that handle's `tracing`→OpenTelemetry metrics bridge installed as the dispatch for the
//! pass (item 2), so every durability-plane metric the loops emit lands in *this role's*
//! provider and is observable through its export surface
//! ([`DurabilityTelemetry::gather_prometheus`] / OTLP). The `wyrd custodian` subcommand
//! (`cli.rs`) constructs one from operator flags, chooses the export backend by
//! [`ExporterConfig`](wyrd_telemetry::ExporterConfig) (no backend hardcoded, ADR-0012),
//! and drives [`Self::run_reconstruction_until`] until Ctrl-C.
//!
//! It installs the bridge **scoped** (per pass, via [`WithSubscriber`]) rather than as a
//! global default, and building the dispatch once at construction and cloning it per pass
//! (`Dispatch` is `Arc`-backed) means every pass records into the same instruments, so one
//! callsite-interest registration covers the role's lifetime. The scoping is what lets the
//! durability tests each own an isolated provider to read back in-process; a single global
//! provider could not.
//!
//! Because a scoped dispatch **replaces** the global default for the future it wraps, the
//! role's dispatch must carry the log layers too — otherwise the loops' non-metric events
//! (the reconstruction audit lines, the malformed-placement NEEDS-HUMAN warning) would
//! still reach no sink even with a global subscriber installed, which is exactly the gap
//! this file used to document. [`CustodianService::with_logging`] therefore composes
//! `EnvFilter` + `fmt` + [`DurabilityTelemetry::metrics_layer`] into one dispatch, and the
//! `wyrd custodian` role uses it (#527). [`CustodianService::new`] stays **metrics-only**:
//! it is the library/test constructor, and a test that reads a metric back has no use for
//! a formatter writing to the harness's stderr.
//!
//! ## Surviving a killed D-server (the day-one fault) — the honest scope
//!
//! The reconstruction plane classifies an *unreachable / timed-out* fetch as **transient**
//! and propagates it (it must not silently convert a reachable-but-flaky fragment into a
//! re-placement, `reconstruction.rs`). Two mechanisms keep the deployable role alive on
//! the day-one kill without depending on that classification changing:
//!
//! 1. the role derives its **live fleet by probing reachability every pass**
//!    ([`live_reconstruction_view`]): a server that fails its health probe is dropped, so
//!    the reconstruction plane reads *around* it (its fragments resolve as missing and are
//!    rebuilt from the ≥ k survivors); and
//! 2. the continuous loop ([`Self::run_reconstruction_until`]) **logs-and-continues** on a
//!    per-pass [`ReconcileError::Store`] fault (a server that dies *after* the probe, a
//!    transient metadata blip), so a mid-pass death degrades the pass, not the process.
//!    Only a [`ReconcileError::Fenced`] (this custodian was superseded) stops the loop.
//!
//! The reachability probe is a **stand-in** for registration/lease-driven fleet membership:
//! the durable answer to "which D-servers are live" is the etcd-backed `Coordination`
//! discovery seam (ADR-0006) — the *other half* of 0015's deployment prerequisite, explicitly
//! out of scope for this slice (brief §"Out of scope"). Whether the probe stand-in is
//! acceptable for the first-deployment gate, or the classification seam should instead treat
//! unreachable-during-reconstruction as missing, is a recorded human/proposal decision
//! carried forward (iteration-3 §C5a) — flagged, not silently resolved here.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::instrument::WithSubscriber;
use tracing::Dispatch;
use tracing_subscriber::prelude::*;

use crate::logging::{self, LogConfig};
use tracing_subscriber::Registry;
use wyrd_core::placement::Topology;
use wyrd_custodian::{
    reconcile_after_restore, reconcile_step, Custodian, FencedZone, GcContext, RebalanceContext,
    ReconcileError, Reconciled, ReconstructionContext, RestoreReport, ScrubContext,
};
use wyrd_telemetry::DurabilityTelemetry;
use wyrd_traits::{BoxError, ChunkStore, DServerId, MetadataStore};

/// The reader-safe grace window the **deployed GC pass** honours: how long an orphaned
/// fragment must outlive its recorded `orphaned_at` deadline before the run loop reclaims
/// its bytes.
///
/// **Derived, not invented — a conservative FLOOR, not a proven reader-safety bound.**
/// `GcContext::grace_window_millis` is documented as coming from reader version-hold /
/// lease semantics, never a magic constant (`gc.rs:57-71`; proposal `0005:585-586`). The
/// checkout has no reader version-hold / maximum-read-duration mechanism yet, so no
/// derivation can *prove* a value reader-safe here — building that bound is a separate work
/// item this bundle does not attempt.
///
/// The system trusts TWO pending-lease timescales, not one: the CLI stamps a 60 s lease
/// ([`crate::cli::LEASE_TTL_MILLIS`]) and the production gateway a 30 s lease
/// (`DEFAULT_LEASE_TTL_MILLIS`, `lib.rs:49`). This reuses the LONGER of the two — the same
/// value the shipped post-restore pass derives (`RESTORE_GRACE_WINDOW_MILLIS =
/// LEASE_TTL_MILLIS`, `cli.rs:68-83`) — so the deployed floor is no shorter than the grace
/// the write path already relies on for either producer. The exact deployed value is a
/// measurement question for the maintainer (`0005:585-586`) — see the build-notes
/// NEEDS-HUMAN sign-off item.
///
/// NOTE: this window gates the ORPHAN input only (`gc.rs:136`). GC's expired-pending input
/// treats the lease TTL itself as its grace (`gc.rs:142-144`); the safety of collecting
/// that input against still-in-flight writers is the routed-back lease-liveness concern
/// recorded in build-notes, not something this constant closes.
const GC_GRACE_WINDOW_MILLIS: u64 = crate::cli::LEASE_TTL_MILLIS;

/// A configured D-server the role was told to maintain over: its stable [`DServerId`],
/// its opaque failure-domain label, and the connected [`ChunkStore`] client. The role
/// probes each one's reachability every pass ([`live_reconstruction_view`]) and hands the
/// reconstruction plane only the live subset.
///
/// The `id` and `failure_domain` are what the reconstruction placement selector keys on.
/// They are the D-server's **own registered identity** — supplied by the operator to match
/// each D-server's `--id` / `--failure-domain` (the day-one runbook pins them), NOT
/// fabricated positionally from the `--endpoints` order. Deriving them automatically from
/// each D-server's registration record awaits the cross-process discovery seam (the
/// out-of-scope etcd `Coordination`); until then the operator supplies the real topology
/// rather than the role inventing one (iteration-3 rejection: "don't invent topology").
///
/// The store is held by **owned** [`Arc`], not a borrow, so the fleet is a self-contained
/// owned value a [`DServerConnector`] can *return* (production wires a real gRPC client; a
/// test injects an in-memory fake) — [`connect_fleet`] assembles it, and it can be moved
/// into and driven by the run loop without the whole assembly having to live inside
/// `cmd_custodian` (iteration-5 BLOCKING #2b: introduce an owned fleet type).
pub struct ConfiguredDServer {
    /// The D-server's stable id (the placement vector keys fragments by this).
    pub id: DServerId,
    /// The D-server's failure-domain label (distinct domains keep a chunk survivable).
    pub failure_domain: String,
    /// The connected store client the role reads/re-places fragments through.
    pub store: Arc<dyn ChunkStore>,
}

/// The seam that turns a D-server **endpoint** into a connected [`ChunkStore`] client — the
/// one concrete-transport call `connect_fleet` makes, abstracted behind a trait so the fleet
/// assembly is coverable headlessly (iteration-5 BLOCKING #2a). Production wires a real gRPC
/// dial ([`crate::cli`]'s `GrpcDServerConnector`, `GrpcChunkStore::connect_with_timeout`); a
/// test injects a fake that returns in-memory stores and can return `Err` for one endpoint to
/// exercise the start-degraded path (BLOCKING #3) without a network.
#[async_trait]
pub trait DServerConnector {
    /// Dial `endpoint`, returning the connected store or a transport error. A returned `Err`
    /// is a *startup-unreachable* peer, which [`connect_fleet`] reads around (it does not
    /// abort fleet assembly).
    async fn connect(
        &self,
        endpoint: &str,
        timeout: Duration,
    ) -> Result<Arc<dyn ChunkStore>, BoxError>;
}

/// Assemble the configured D-server fleet from the operator's `endpoints` / `ids` /
/// `domains`, dialing each through the injected `connector` — the single testable place the
/// custodian's fleet is built (iteration-5 BLOCKING #2c: `require_aligned_topology` + the
/// dial loop + the `id`/`failure_domain` mapping in one function, not inlined in
/// `cmd_custodian`).
///
/// **Topology is never fabricated:** `require` rejects any missing / mismatched `ids` /
/// `domains` (the caller passes [`crate::cli`]'s `require_aligned_topology`), so a rebuilt
/// fragment can never be re-placed onto a survivor's real failure domain (iteration-4
/// rejection).
///
/// **Start-degraded (iteration-5 BLOCKING #3):** a peer that is *unreachable at startup* — a
/// D-server killed before or during the very day-one incident the custodian exists to repair
/// (architecture §7.4 step 4) — must NOT abort the whole role. Its `connect` `Err` is logged
/// and the endpoint is **skipped**, so the role starts on the reachable subset and repairs
/// *around* the down peer, exactly as `live_reconstruction_view` reads around a peer that
/// dies mid-run. Returning the reachable subset (rather than propagating the first `Err`) is
/// what makes a custodian *started or restarted during* the incident come up and repair,
/// instead of exiting on the fault it is meant to fix.
pub async fn connect_fleet<C, R>(
    connector: &C,
    endpoints: &[String],
    ids: &[u64],
    domains: &[String],
    timeout: Duration,
    require: R,
) -> Result<Vec<ConfiguredDServer>, BoxError>
where
    C: DServerConnector + ?Sized,
    R: FnOnce(usize, &[u64], &[String]) -> Result<(), BoxError>,
{
    require(endpoints.len(), ids, domains)?;
    let mut fleet = Vec::with_capacity(endpoints.len());
    for (i, endpoint) in endpoints.iter().enumerate() {
        match connector.connect(endpoint, timeout).await {
            Ok(store) => fleet.push(ConfiguredDServer {
                id: ids[i] as DServerId,
                failure_domain: domains[i].clone(),
                store,
            }),
            // Start-degraded: a startup-unreachable peer is read around, not fatal.
            Err(e) => eprintln!(
                "wyrd custodian: D server `{endpoint}` unreachable at startup ({e}); \
                 starting degraded and repairing around it"
            ),
        }
    }
    Ok(fleet)
}

/// Probe every configured D-server and return the **reachable** subset as the reconstruction
/// view — the `(fleet, topology, unreachable)` a [`ReconstructionContext`] reads over. The
/// third element is the set of configured servers that FAILED the probe this pass: their
/// placed fragments are *transiently* unavailable, so [`ReconstructionContext::unreachable`]
/// lets `assess` distinguish "unreachable right now" from "fragments confirmed gone" and NOT
/// raise a false data-loss alarm on a below-`k` shortfall a transient outage alone explains
/// (iteration-7 MUST-FIX).
///
/// This is the wiring that makes the deployable role SURVIVE the architecture §7.4 day-one
/// step-4 fault (kill a D-server), driving the *same* production path the binary runs (the
/// fleet is built from every configured endpoint, **including** the one that dies — the
/// kill is handled here, at the role boundary, not by curating it out of the input). A
/// killed / unreachable gRPC D-server answers its health probe with a transport error;
/// were it left in the fleet, the first `get_fragment` the reconstruction assessment issues
/// against it would raise a transient fault that `reconstruction::assess` propagates,
/// unwinding the whole pass. Dropping unreachable servers here lets the reconstruction plane
/// read *around* the loss (the dropped server's fragments resolve as missing and are rebuilt
/// from the ≥ k survivors) so the role keeps running.
///
/// A server that answers its probe at all — `Healthy`, `Degraded`, or even `Unhealthy` —
/// stays in the fleet: it is reachable, so its per-fragment faults are the
/// permanent-vs-transient concern `assess` already handles. Only an *unreachable* server is
/// the fatal case, and only it is dropped. The topology is built from the same live subset,
/// so a rebuilt fragment is never re-placed onto a domain that just went dark. The
/// topology also uses each server's operator-supplied `failure_domain`, so a rebuilt
/// fragment respects the real cross-domain distinctness, not a fabricated one.
pub async fn live_reconstruction_view(
    configured: &[ConfiguredDServer],
) -> (Vec<(DServerId, &dyn ChunkStore)>, Topology, Vec<DServerId>) {
    let mut fleet = Vec::with_capacity(configured.len());
    let mut topology = Topology::default();
    let mut unreachable = Vec::new();
    for d in configured {
        // Reachable at all (any `Ok(Health)`) → keep; unreachable (`Err`) → drop and read
        // around. `health()` is the same readiness signal proposal 0010 item 7's probe
        // reflects, reused here so "is this server serving?" has one answer.
        if d.store.health().await.is_ok() {
            fleet.push((d.id, d.store.as_ref()));
            topology.register(d.id, d.failure_domain.clone());
        } else {
            // Dropped as unreachable THIS pass. Its placed fragments are transiently
            // unavailable — NOT confirmed lost — so the reconstruction plane must not raise
            // the high-severity data-loss alarm on a below-`k` shortfall this server alone
            // explains (iteration-7 MUST-FIX). The set is handed to the pass so `assess` can
            // tell "unreachable right now" apart from "fragments confirmed gone".
            unreachable.push(d.id);
        }
    }
    (fleet, topology, unreachable)
}

/// A running custodian role: it owns the durability-plane [`DurabilityTelemetry`] handle
/// and drives the fenced reconciliation loop with that handle's metrics bridge installed,
/// so the durability metrics a pass emits are captured by this role's provider and
/// observable through its export surface. This is the wiring proposal 0010 items 1–2
/// deliver — the library maintenance plane made a *runnable process* that installs the
/// telemetry seam at entry and runs the leader-elected loop through it.
pub struct CustodianService {
    telemetry: DurabilityTelemetry,
    /// The role's `tracing` dispatch, built **once** over this role's telemetry provider,
    /// so every pass records into the same instruments (and one callsite-interest
    /// registration covers the whole role's lifetime). Cheap to clone (an `Arc`);
    /// installed scoped per pass, never as a global default.
    dispatch: Dispatch,
}

impl CustodianService {
    /// Wire a role over `telemetry` — the shared telemetry handle installed at role entry
    /// (proposal 0010 item 1). The handle's export surface(s) were chosen by the caller's
    /// [`ExporterConfig`](wyrd_telemetry::ExporterConfig) (no backend hardcoded here,
    /// ADR-0012); this role only routes the loops' emission into it.
    pub fn new(telemetry: DurabilityTelemetry) -> Self {
        let dispatch = Dispatch::new(Registry::default().with(telemetry.metrics_layer()));
        Self {
            telemetry,
            dispatch,
        }
    }

    /// Wire a role over `telemetry` **and** the operational log sink (#527) — the
    /// constructor the deployable `wyrd custodian` binary uses.
    ///
    /// [`Self::new`] gives a metrics-only dispatch, which is right for a test that reads a
    /// metric back but wrong for a running role: a scoped dispatch replaces the global
    /// default for the pass it wraps, so with a metrics-only dispatch the loops' audit
    /// lines — `emit_data_loss`'s *"DATA IS LOST; NEEDS-HUMAN"* among them — would be
    /// swallowed inside every pass even though the process has a global subscriber. This
    /// constructor composes the log layers into the same dispatch, so a pass emits both.
    pub fn with_logging(telemetry: DurabilityTelemetry, log: &LogConfig) -> Self {
        Self::with_logging_to(telemetry, log, std::io::stderr)
    }

    /// [`Self::with_logging`] over an arbitrary writer. The writer is a seam so a test can
    /// assert *what the role actually emits* — the only way to prove the audit lines reach
    /// a sink rather than trusting that they do.
    pub fn with_logging_to<W>(telemetry: DurabilityTelemetry, log: &LogConfig, writer: W) -> Self
    where
        W: for<'w> tracing_subscriber::fmt::MakeWriter<'w> + Send + Sync + 'static,
    {
        let dispatch = logging::dispatch(log, writer, telemetry.metrics_layer());
        Self {
            telemetry,
            dispatch,
        }
    }

    /// This role's durability-plane telemetry handle — the read-back / export surface a
    /// scrape endpoint exposes, and the in-process
    /// [`DurabilityTelemetry::gather_prometheus`] the day-one signal is asserted through.
    pub fn telemetry(&self) -> &DurabilityTelemetry {
        &self.telemetry
    }

    /// Run **one** fenced reconciliation pass with this role's telemetry bridge installed
    /// as the `tracing` dispatch for the pass (proposal 0010 item 2). It delegates to the
    /// real [`reconcile_step`] fenced control point — the *same* code path the M3 property
    /// campaign drives, never a parallel entry (the anti-#141 guard) — and adds only the
    /// telemetry wiring, so the durability metrics the loops emit
    /// (`reconstruction_under_replicated`, queue depth, …) are captured by
    /// [`Self::telemetry`] and observable through its export surface.
    #[allow(clippy::too_many_arguments)]
    pub async fn reconcile_pass(
        &self,
        zone: &FencedZone,
        custodian: &Custodian,
        gc: Option<&GcContext<'_>>,
        scrub: Option<&ScrubContext<'_>>,
        reconstruction: Option<&ReconstructionContext<'_>>,
        rebalance: Option<&RebalanceContext<'_>>,
        now_millis: u64,
    ) -> Result<Reconciled, ReconcileError> {
        reconcile_step(
            zone,
            custodian,
            gc,
            scrub,
            reconstruction,
            rebalance,
            now_millis,
        )
        .with_subscriber(self.dispatch.clone())
        .await
    }

    /// One **post-restore reconciliation** pass over the configured fleet (#551) — the
    /// operator command run after a metadata restore, with the writers stopped.
    ///
    /// A restore rewinds the metadata while the D servers stay at "now", and `gc` reclaims a
    /// fragment only on EVIDENCE of an elapsed grace deadline — an `orphan:` record or an
    /// expired `pending:` lease — both of which lived in the metadata the restore erased. So
    /// post-restore strays are unreferenced *and* evidence-free, and GC keeps them forever.
    /// This pass supplies the missing evidence, and reports the chunks whose bytes were
    /// already reclaimed before the restore resurrected their maps.
    ///
    /// Runs through the role's dispatch, so its audit lines — the per-fragment marks and the
    /// `DANGLING` operator signal above all — land on the same telemetry surface as the loops.
    ///
    /// **Marks; never deletes.** GC does the reclaiming, on its own grace window, later.
    pub async fn reconcile_after_restore_pass(
        &self,
        meta: &dyn MetadataStore,
        configured: &[ConfiguredDServer],
        grace_window_millis: u64,
        now_millis: u64,
    ) -> Result<RestoreReport, BoxError> {
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = configured
            .iter()
            .map(|d| (d.id, d.store.as_ref()))
            .collect();
        let gc = GcContext {
            meta,
            fleet: &fleet,
            grace_window_millis,
        };
        let report = reconcile_after_restore(&gc, now_millis)
            .with_subscriber(self.dispatch.clone())
            .await?;
        Ok(report)
    }

    /// The deployable **reconstruction run loop** — the spine the `wyrd custodian` binary
    /// drives over a configured D-server fleet. Each pass:
    ///
    /// 1. derives the LIVE reconstruction view ([`live_reconstruction_view`]) — dropping
    ///    any D-server that fails its reachability probe, so a killed server is read
    ///    *around* rather than crashing the pass;
    /// 2. runs up to three fenced [`Self::reconcile_pass`] calls — first a **best-effort
    ///    scrub** of the live fleet to DERIVE this store's repair obligations from the committed
    ///    (gateway-written) placement ([`wyrd_custodian::scrub`] enqueues any referenced
    ///    fragment it finds absent/corrupt), then a **reconstruction** pass that assesses and
    ///    repairs those obligations (the durability metrics land on this role's export
    ///    surface). Scrub's enqueue is persisted to the shared store, so the separate
    ///    reconstruction pass still drains it in the same interval. Deriving the obligations
    ///    here is what closes the write→durability loop: without the scrub half the loop would
    ///    only drain obligations some *other* producer enqueued, so a custodian opened over
    ///    the store a gateway wrote would compute NO repair work from the placement — the
    ///    "empty store sees zero repair" symptom (#455). Finally, when the loop can see the
    ///    **WHOLE operator-configured fleet** this pass — every one of the `operator_fleet_size`
    ///    endpoints the operator wired, connected AND reachable — a **garbage-collection** pass
    ///    ([`wyrd_custodian::gc`]) reclaims the bytes of fragments a delete/overwrite orphaned
    ///    once their recorded reader-safe grace deadline elapsed. GC is the ONLY collector of
    ///    those bytes, so without this pass every delete/overwrite leaks its displaced fragments
    ///    forever (#554). GC is DEFERRED whenever ANY configured server is missing from the live
    ///    view — whether it never connected at startup (`connect_fleet` starts DEGRADED, dropping
    ///    a startup-unreachable peer, so `configured` itself can be short of the operator fleet)
    ///    or it dropped its reachability probe this pass: GC's expired-pending input retires
    ///    CHUNK-WIDE evidence, so sweeping a partial fleet could retire the sole record for a
    ///    fragment a missing server still holds and strand it forever once that server returns.
    ///    The gate is therefore `live_fleet.len() == operator_fleet_size` (not merely
    ///    `unreachable.is_empty()`, which only sees servers that dropped AFTER a successful
    ///    startup connect — the #554-iteration-2 startup-partial hazard). Deferring preserves
    ///    every orphan/pending record for a later whole-fleet pass, so a skipped server's garbage
    ///    is reaped once the fleet is whole and "skipped" is never mistaken for "collected".
    ///
    ///    TRADE-OFF (maintainer-visible, #554 §6): this gate is conservative in the pause
    ///    direction — ANY single configured D-server that is unreachable OR decommissioned but
    ///    still listed in `--endpoints` pauses ALL reclamation, fleet-wide, for as long as it is
    ///    absent. That is deliberate: a false "collected" is a permanent, silent byte leak,
    ///    whereas a paused reclaim is recovered in full on the next whole-fleet pass. Relaxing it
    ///    to reclaim orphans over the reachable subset needs GC to preserve pending evidence
    ///    per-server (the routed-back lease-liveness / #490 work), which this bundle does not do;
    /// 3. **survives** a per-pass store fault — a [`ReconcileError::Store`] (a server that
    ///    died *after* the probe, a transient metadata blip) is logged to stderr and the loop
    ///    continues. Crucially, a scrub-pass fault is isolated from reconstruction: it is
    ///    logged and reconstruction still drains the backlog, so one reachable-but-flaky
    ///    node/object cannot stall ALL repair every interval (Codex #461). Only a
    ///    [`ReconcileError::Fenced`] (this custodian was superseded — a newer term holds the
    ///    zone) stops the loop, since continuing would be an unfenced actor.
    ///
    /// This is the production wiring the day-one runbook exercises: kill a D-server, watch
    /// the under-replicated gauge rise then return to zero, through a role that does not
    /// exit on the kill. `now_millis` is advanced by the caller's wall `clock`.
    ///
    /// `operator_fleet_size` is the number of D-server endpoints the OPERATOR wired
    /// (`--endpoints`), which the caller must pass separately from `configured`: `connect_fleet`
    /// starts degraded and may hand this loop a `configured` slice SHORTER than the operator
    /// fleet (a peer down at startup is dropped), and only GC needs to know the difference — it
    /// gates on seeing every operator endpoint, exactly as the #551 restore pass refuses a
    /// partial fleet (`cli.rs:961-975`). The reconstruction/scrub passes are unaffected (they
    /// read around any absent peer, by design).
    #[allow(clippy::too_many_arguments)]
    pub async fn run_reconstruction_until<Fut, Clock>(
        &self,
        zone: &FencedZone,
        custodian: &Custodian,
        meta: &dyn MetadataStore,
        configured: &[ConfiguredDServer],
        operator_fleet_size: usize,
        interval: Duration,
        mut clock: Clock,
        shutdown: Fut,
    ) -> Result<(), ReconcileError>
    where
        Fut: Future<Output = ()>,
        Clock: FnMut() -> u64,
    {
        tokio::pin!(shutdown);
        loop {
            let (fleet, topology, unreachable) = live_reconstruction_view(configured).await;
            // DERIVE this pass's repair obligations from the committed (gateway-written)
            // placement rather than only draining ones some other producer already enqueued.
            // The scrub loop walks every referenced fragment on the live fleet and enqueues
            // any it finds absent or corrupt ([`wyrd_custodian::scrub`]), so the SAME pass's
            // reconstruction then assesses and repairs them. This is the write→repair join
            // the deployable role needs: the placement a gateway PUT records over the cluster
            // store is exactly what scrub reads to compute that object's repair obligations —
            // ONE placement contract shared by the write path and the repair scan. Without it
            // the reconstruction plane would run over an EMPTY queue on a store nothing else
            // scrubbed, and a custodian opened over the store a gateway wrote would see zero
            // repair work (the "empty store" symptom, #455). Scrub runs over the LIVE
            // (reachable) fleet — a wholly-unreachable peer is dropped by the reachability
            // probe and read *around* by reconstruction, unchanged — so a transient scrub
            // fault degrades ONLY scrub (logged below), never reconstruction or the process.
            let scrub_ctx = ScrubContext {
                meta,
                fleet: &fleet,
            };
            let ctx = ReconstructionContext {
                meta,
                fleet: &fleet,
                topology: &topology,
                // The servers dropped as unreachable this pass: `assess` treats their placed
                // fragments as transiently unavailable, not confirmed lost (no false data-loss).
                unreachable: &unreachable,
            };
            // Scrub and reconstruction run as TWO passes, not one. `reconcile_step` short-
            // circuits on the first loop's error (scrub's `?`), so a combined pass would let a
            // transient scrub fault — a reachable node erroring on a single `get_fragment` —
            // abort before reconstruction runs, stalling the ENTIRE repair backlog every
            // interval until a full scrub succeeds (Codex #461). Scrub enqueues into the shared
            // store, which persists, so a separate reconstruction pass still drains this pass's
            // enqueues (same-interval drain preserved). Scrub therefore runs BEST-EFFORT: its
            // store fault is logged and reconstruction proceeds; a superseded term still stops.
            match self
                .reconcile_pass(zone, custodian, None, Some(&scrub_ctx), None, None, clock())
                .await
            {
                Ok(_) => {}
                Err(e @ ReconcileError::Fenced(_)) => return Err(e),
                Err(ReconcileError::Store(e)) => {
                    eprintln!(
                        "wyrd custodian: scrub pass degraded (repair backlog still drained): {e}"
                    );
                }
            }
            // Reconstruction pass — drains the shared repair queue (what scrub just enqueued
            // plus any prior backlog), independent of a scrub blip.
            match self
                .reconcile_pass(zone, custodian, None, None, Some(&ctx), None, clock())
                .await
            {
                Ok(_) => {}
                // A superseded term must stop reconciling — it is fenced from committing
                // anyway, but it should not keep burning passes as a zombie.
                Err(e @ ReconcileError::Fenced(_)) => return Err(e),
                // A transient store fault (a server that died after the probe, a metadata
                // blip) degrades the pass, not the process: log and try again next tick.
                Err(ReconcileError::Store(e)) => {
                    eprintln!(
                        "wyrd custodian: reconstruction pass failed (retrying next interval): {e}"
                    );
                }
            }
            // GARBAGE-COLLECTION pass — reclaim the bytes of fragments a delete
            // (`metadata::unlink`) or overwrite left unreferenced, once their recorded
            // reader-safe grace deadline has elapsed. GC is the ONLY collector of those bytes;
            // the write path deliberately marks-not-deletes so a reader inside the grace window
            // is never torn (`metadata.rs:350-368`). Without this pass the deployed role never
            // reclaims anything and every delete/overwrite leaks its displaced bytes forever
            // (#554).
            //
            // FLEET-VIEW SAFETY (#554 iteration-1 C3/C5/T3 + iteration-2 startup-partial
            // correction): GC runs ONLY when the loop can see the WHOLE operator-configured fleet
            // this pass — `fleet.len() == operator_fleet_size`. GC's expired-pending input retires
            // CHUNK-WIDE evidence — it deletes the `pending:` ledger entry for every chunk it
            // swept a copy of (`gc.rs:155-167`) — so sweeping a PARTIAL fleet could retire the
            // sole evidence for a fragment a MISSING server still holds, stranding it forever once
            // that server returns.
            //
            // The gate is `fleet.len() == operator_fleet_size`, NOT `unreachable.is_empty()`. The
            // latter (iteration-2's gate) is defeated at STARTUP: `connect_fleet` starts degraded
            // and silently drops a peer that is unreachable when the custodian boots
            // (`custodian.rs` `connect_fleet`; `cli.rs`), so `configured` is ALREADY short of the
            // operator fleet and `live_reconstruction_view(configured)` returns an empty
            // `unreachable` — the first GC pass would then retire chunk-wide `pending:` evidence
            // for a fragment the never-connected server still holds, the exact permanent leak the
            // GC deferral exists to prevent (iteration-2 rejection). Gating on the operator fleet
            // size closes BOTH holes at once: a startup-omitted server (never in `configured`) and
            // a runtime-unreachable server (dropped this pass) both make `fleet.len()` fall short.
            // Since `fleet ⊆ configured` and `configured.len() ≤ operator_fleet_size`,
            // `fleet.len() == operator_fleet_size` holds IFF every operator endpoint is connected
            // AND reachable this pass. This mirrors the #551 restore pass, which refuses a partial
            // fleet by comparing `configured.len()` against the operator endpoint count
            // (`cli.rs:961-975`).
            //
            // Deferring preserves every orphan/pending record untouched, so a missing server's
            // garbage is reaped on a later whole-fleet pass and "skipped" is never mistaken for
            // "collected". Conservative in the pause direction (see the run-loop doc trade-off):
            // any single absent/decommissioned-but-configured server pauses ALL reclamation until
            // the fleet is whole, but a paused reclaim is fully recovered whereas a false
            // "collected" is a permanent silent leak.
            //
            // A DISTINCT fenced pass, not folded into scrub/reconstruction: `reconcile_step`
            // runs GC FIRST within a combined pass and short-circuits on the first `?`, so
            // folding GC in would let a GC store fault suppress scrub/reconstruction for the
            // interval and be mislabelled — the fault-isolation rule that split scrub from
            // reconstruction (Codex #461). Isolated here, a GC store fault degrades ONLY GC.
            //
            // Ordered LAST, after reconstruction committed any placement rewrite: the passes run
            // sequentially (no concurrency) and GC gates every reclaim on the freshly-committed
            // reference set (`gc::referenced_fragments` never reclaims a referenced fragment), so
            // it can neither race nor reclaim a fragment reconstruction just re-placed. The grace
            // window is DERIVED inside the role from the lease TTL ([`GC_GRACE_WINDOW_MILLIS`]) —
            // never a magic constant — exactly as the post-restore pass derives it
            // (`cli.rs:68-83`).
            if fleet.len() == operator_fleet_size {
                let gc_ctx = GcContext {
                    meta,
                    fleet: &fleet,
                    grace_window_millis: GC_GRACE_WINDOW_MILLIS,
                };
                match self
                    .reconcile_pass(zone, custodian, Some(&gc_ctx), None, None, None, clock())
                    .await
                {
                    Ok(_) => {}
                    // A superseded term must stop reconciling — continuing would be an
                    // unfenced actor.
                    Err(e @ ReconcileError::Fenced(_)) => return Err(e),
                    // A transient store fault degrades ONLY this GC pass (scrub and
                    // reconstruction already ran): log and retry the reclaim next interval.
                    Err(ReconcileError::Store(e)) => {
                        eprintln!(
                            "wyrd custodian: gc pass degraded (bytes reclaimed next interval): {e}"
                        );
                    }
                }
            } else {
                // Defer GC: the loop cannot see the whole operator fleet this pass (a server is
                // unreachable now, or never connected at startup), so sweeping could retire
                // chunk-wide pending evidence for a fragment a missing server still holds. All
                // evidence is preserved for the next whole-fleet pass.
                eprintln!(
                    "wyrd custodian: gc pass deferred ({} of {} operator-configured d-server(s) \
                     visible; evidence preserved, reclaimed on a later whole-fleet pass)",
                    fleet.len(),
                    operator_fleet_size,
                );
            }
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }
}
