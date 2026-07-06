//! The `wyrd` command-line frontend over M0's in-process write/read paths —
//! the M0.8 round trip turned into commands a human can run. Dev/test tooling,
//! not a network surface (M0 has none): the backends are driven directly via
//! `pollster::block_on`.
//!
//! Stream discipline: `get` writes the object's raw bytes to **stdout**, so all
//! diagnostics (errors, usage, summaries with no payload) go to **stderr** and a
//! redirect like `wyrd get k > out.bin` is never corrupted. No logging crate —
//! observability is deferred past M0 (ADR-0012).

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use pollster::block_on;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{FanoutChunkStore, GrpcChunkStore};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{ChunkId, CommitOutcome, MetadataStore, PlacementChunkStore, WriteBatch};

use crate::dserver::{self, DServer};
use crate::{Gateway, DEFAULT_DURABILITY};
use wyrd_gateway_s3 as s3;

/// Default endpoint the `d-server` role binds and advertises.
const DEFAULT_DSERVER_BIND: &str = "127.0.0.1:50051";
/// Default listen address for the `s3` gateway role. `0.0.0.0:8080` mirrors the
/// blueprint's `--s3-listen 0.0.0.0:8080` (m4-first-deployment-blueprint:620-623).
const DEFAULT_S3_LISTEN: &str = "0.0.0.0:8080";
/// Default SigV4 region the `s3` role expects in a credential scope.
const DEFAULT_S3_REGION: &str = "us-east-1";
/// Default D-server registration lease lifetime, and how often it is renewed —
/// renew well within the TTL so a missed tick does not drop the registration.
const DEFAULT_DSERVER_LEASE_TTL_SECS: u64 = 30;
const DEFAULT_DSERVER_RENEW_SECS: u64 = 10;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Every object key binds under the root inode (a flat namespace at M0).
const ROOT: u64 = 0;
/// The persisted, monotonic inode-id allocator, so `put` and `get` (separate
/// processes) agree on ids across invocations.
const NEXT_INODE_KEY: &[u8] = b"meta:next_inode";
const DEFAULT_DATA_DIR: &str = "wyrd-data";
const DEFAULT_CHUNK_SIZE: usize = 1 << 20;
// The CLI runs no custodian sweep, so lease expiry is moot once the commit
// releases the pending ledger; a fixed time keeps runs reproducible.
const NOW_MILLIS: u64 = 0;
const LEASE_TTL_MILLIS: u64 = 60_000;

// Bounded retry-with-backoff for [`alloc_inode`] (proposal 0015 §"Composition,
// not refactor" item 3). Over embedded redb a conflicting commit is a sub-µs
// local retry, so the old unbounded spin was harmless; over distributed TiKV
// every attempt is a network round-trip, so the spin is a latency/load footgun.
// The retries are therefore spaced with capped exponential backoff and bounded by
// a WALL-CLOCK BUDGET rather than a fixed attempt count: the inode counter is a
// single hot key, so a normal burst of N concurrent creates serialises one-per-round
// against it, and a fixed count (say 8) would reject the (count+1)-th writer with a
// spurious "persistent conflict" though the backend is healthy (Codex P2, #427). A
// time budget instead retries a *contended* allocation until it succeeds and reserves
// the error for a backend that genuinely cannot make progress within the budget.
const ALLOC_INODE_BUDGET: Duration = Duration::from_secs(2);
const ALLOC_INODE_BASE_BACKOFF: Duration = Duration::from_millis(2);
const ALLOC_INODE_MAX_BACKOFF: Duration = Duration::from_millis(64);
// Cap the backoff-exponent shift so `1u32 << attempt` can't overflow once the loop is
// bounded by time (base 2ms << 5 == 64ms already saturates ALLOC_INODE_MAX_BACKOFF).
const ALLOC_INODE_MAX_SHIFT: u32 = 5;

/// The metadata backend `server` composes behind the unchanged `MetadataStore`
/// seam, chosen by configuration (proposal 0015 §"Backend selection in `server`",
/// §"Suggested PR sequence" slice 4). This is the composition change M4 exists to
/// demonstrate: selecting a backend is "pass a different concrete", not a refactor
/// of any consumer (ADR-0008/0016).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataBackend {
    /// Embedded redb — the dev / single-binary default, carrying no production
    /// durability promise (ADR-0014). Always available.
    Redb,
    /// Distributed TiKV — the production backend (ADR-0008). Compiled only under
    /// the OFF-by-default `tikv` feature, which forwards to `metadata-tikv`'s own
    /// `tikv` feature, so the default build never pulls the `tikv-client` tree.
    #[cfg(feature = "tikv")]
    Tikv,
}

impl MetadataBackend {
    /// Select the backend from a config value (the `--metadata-backend` flag or the
    /// `WYRD_METADATA_BACKEND` env var). Absent ⇒ the redb dev default (ADR-0014).
    /// `tikv` is only accepted when the binary was built `--features tikv`.
    pub fn from_config(value: Option<&str>) -> Result<Self, BoxError> {
        match value {
            None | Some("redb") => Ok(Self::Redb),
            #[cfg(feature = "tikv")]
            Some("tikv") => Ok(Self::Tikv),
            #[cfg(not(feature = "tikv"))]
            Some("tikv") => Err(
                "metadata backend `tikv` requires building `wyrd` with `--features tikv`".into(),
            ),
            Some(other) => Err(format!(
                "unknown metadata backend `{other}` (expected `redb` or `tikv`)"
            )
            .into()),
        }
    }
}

/// Resolve the metadata backend from config: the `--metadata-backend` flag wins,
/// else the `WYRD_METADATA_BACKEND` env var, else the redb dev default.
fn resolve_backend(parsed: &ParsedArgs) -> Result<MetadataBackend, BoxError> {
    let value = match parsed.flag("metadata-backend") {
        Some(flag) => Some(flag.to_string()),
        None => std::env::var("WYRD_METADATA_BACKEND").ok(),
    };
    MetadataBackend::from_config(value.as_deref())
}

/// Connect the production TiKV metadata store from its PD endpoints
/// (`WYRD_TIKV_PD_ENDPOINTS`, comma-separated). Compiled only under the `tikv`
/// feature — the tikv selection arm is `#[cfg]`-gated out of the default build.
#[cfg(feature = "tikv")]
async fn open_tikv_meta() -> Result<wyrd_metadata_tikv::TikvMetadataStore, BoxError> {
    let raw = std::env::var("WYRD_TIKV_PD_ENDPOINTS").map_err(|_| {
        "tikv backend: set WYRD_TIKV_PD_ENDPOINTS to the PD endpoints (comma-separated)"
    })?;
    let endpoints = parse_endpoints(&raw)?;
    Ok(wyrd_metadata_tikv::TikvMetadataStore::connect(endpoints).await?)
}

/// The L5 `Coordination` backend `server` composes behind the unchanged
/// `Coordination` seam, chosen by configuration (ADR-0006, proposal 0015
/// §"Deployment prerequisite", #365). Selecting it is "pass a different concrete",
/// not a refactor of any consumer (ADR-0008/0016) — the mirror of
/// [`MetadataBackend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinationBackend {
    /// Process-local in-memory coordination — the dev / single-binary default; a
    /// D server in a separate process is not discoverable by a separate gateway.
    Mem,
    /// etcd-backed coordination (ADR-0006): cross-process discovery, single-leader
    /// election, and fencing across machines. Compiled only under the OFF-by-default
    /// `etcd` feature, which forwards to `coordination-etcd`'s own `etcd` feature,
    /// so the default build never pulls the `etcd-client` tree.
    #[cfg(feature = "etcd")]
    Etcd,
}

impl CoordinationBackend {
    /// Select the backend from a config value (the `--coordination-backend` flag or
    /// the `WYRD_COORDINATION_BACKEND` env var). Absent ⇒ the in-memory dev default.
    /// `etcd` is only accepted when the binary was built `--features etcd`.
    pub fn from_config(value: Option<&str>) -> Result<Self, BoxError> {
        match value {
            None | Some("mem") => Ok(Self::Mem),
            #[cfg(feature = "etcd")]
            Some("etcd") => Ok(Self::Etcd),
            #[cfg(not(feature = "etcd"))]
            Some("etcd") => Err(
                "coordination backend `etcd` requires building `wyrd` with `--features etcd`"
                    .into(),
            ),
            Some(other) => Err(format!(
                "unknown coordination backend `{other}` (expected `mem` or `etcd`)"
            )
            .into()),
        }
    }
}

/// Resolve the coordination backend from config: the `--coordination-backend` flag
/// wins, else the `WYRD_COORDINATION_BACKEND` env var, else the in-memory default.
fn resolve_coordination_backend(parsed: &ParsedArgs) -> Result<CoordinationBackend, BoxError> {
    let value = match parsed.flag("coordination-backend") {
        Some(flag) => Some(flag.to_string()),
        None => std::env::var("WYRD_COORDINATION_BACKEND").ok(),
    };
    CoordinationBackend::from_config(value.as_deref())
}

/// Connect the production etcd Coordination from its endpoints
/// (`WYRD_ETCD_ENDPOINTS`, comma-separated). Compiled only under the `etcd`
/// feature — the etcd selection arm is `#[cfg]`-gated out of the default build.
#[cfg(feature = "etcd")]
async fn open_etcd_coordination() -> Result<wyrd_coordination_etcd::EtcdCoordination, BoxError> {
    let raw = std::env::var("WYRD_ETCD_ENDPOINTS").map_err(|_| {
        "etcd backend: set WYRD_ETCD_ENDPOINTS to the etcd endpoints (comma-separated)"
    })?;
    let endpoints = parse_endpoints(&raw)?;
    Ok(wyrd_coordination_etcd::EtcdCoordination::connect(&endpoints).await?)
}

/// Parse `args` (including argv[0]) and run the requested command, returning the
/// process exit code.
pub fn run(args: impl Iterator<Item = String>) -> ExitCode {
    let args: Vec<String> = args.skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("put") => cmd_put(&args[1..]),
        Some("get") => cmd_get(&args[1..]),
        Some("d-server") => cmd_d_server(&args[1..]),
        Some("s3") => cmd_s3(&args[1..]),
        Some("demo") => cmd_demo(),
        Some(other) => {
            eprintln!("wyrd: unknown command `{other}`");
            usage();
            return ExitCode::from(2);
        }
        None => {
            usage();
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("wyrd: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage() {
    eprintln!("usage:");
    eprintln!("  wyrd put <file> --key <name> [--data-dir DIR] [--chunk-size N] [--durability rs(k,m)|none] [--endpoints URL,URL,…] [--metadata-backend redb|tikv]");
    eprintln!("  wyrd get <key> [--out <file>] [--data-dir DIR] [--endpoints URL,URL,…] [--metadata-backend redb|tikv]");
    eprintln!("  wyrd d-server [--bind ADDR] [--data-dir DIR] [--group NAME] [--lease-ttl-secs N] [--renew-secs N] [--coordination-backend mem|etcd]");
    eprintln!("  wyrd s3 --access-key KEY --secret-key SECRET [--s3-listen ADDR] [--data-dir DIR] [--region NAME]");
    eprintln!("  wyrd demo");
    eprintln!();
    eprintln!("  --endpoints drives a local distributed cluster: fragments fan out over gRPC");
    eprintln!(
        "  to the listed D servers (metadata held locally). See README \"Run a local cluster\"."
    );
}

/// Parse a `--durability` value: `rs(k,m)` (Reed-Solomon) or `none`/`replication(1)`.
fn parse_durability(s: &str) -> Result<EcScheme, BoxError> {
    if s == "none" || s == "replication(1)" {
        return Ok(EcScheme::None);
    }
    if let Some(inner) = s.strip_prefix("rs(").and_then(|r| r.strip_suffix(')')) {
        let (k, m) = inner
            .split_once(',')
            .ok_or_else(|| format!("invalid --durability `{s}` (expected `rs(k,m)`)"))?;
        let k: u8 = k
            .trim()
            .parse()
            .map_err(|_| format!("invalid k in --durability `{s}`"))?;
        let m: u8 = m
            .trim()
            .parse()
            .map_err(|_| format!("invalid m in --durability `{s}`"))?;
        if k == 0 {
            return Err("--durability k must be at least 1".into());
        }
        return Ok(EcScheme::ReedSolomon { k, m });
    }
    Err(format!("invalid --durability `{s}` (expected `rs(k,m)` or `none`)").into())
}

/// A short human label for a scheme, for the `put` summary.
fn durability_label(scheme: EcScheme) -> String {
    match scheme {
        EcScheme::None => "none".to_string(),
        EcScheme::ReedSolomon { k, m } => format!("rs({k},{m})"),
    }
}

/// `wyrd put <file> --key <name>`: read the file and write it under `key`.
fn cmd_put(args: &[String]) -> Result<ExitCode, BoxError> {
    let parsed = ParsedArgs::parse(args)?;
    let file = parsed
        .positional(0)
        .ok_or("put: a <file> argument is required")?;
    let key = parsed.flag("key").ok_or("put: --key <name> is required")?;
    let data_dir = parsed.flag("data-dir").unwrap_or(DEFAULT_DATA_DIR);
    let chunk_size = match parsed.flag("chunk-size") {
        Some(s) => s
            .parse()
            .map_err(|_| format!("put: invalid --chunk-size `{s}`"))?,
        None => DEFAULT_CHUNK_SIZE,
    };
    let durability = match parsed.flag("durability") {
        Some(s) => parse_durability(s)?,
        None => DEFAULT_DURABILITY,
    };

    let data =
        std::fs::read(file).map_err(|e| format!("put: cannot read input file `{file}`: {e}"))?;

    let backend = resolve_backend(&parsed)?;

    // Cluster client mode: fan the object's fragments out over gRPC to the
    // configured D servers, holding metadata locally under `data_dir`.
    if let Some(raw) = parsed.flag("endpoints") {
        let endpoints = parse_endpoints(raw)?;
        return cluster_put(
            backend, data_dir, &endpoints, key, &data, chunk_size, durability,
        );
    }

    // Local-disk path. The metadata backend is selected by config, and the paths
    // run on the tokio runtime the cluster paths already use — a `tokio`-bound
    // TiKV client cannot run under `pollster::block_on` (proposal 0015
    // §"Composition, not refactor" item 2).
    let chunks = open_local_chunks(data_dir)?;
    let runtime = tokio_runtime()?;
    runtime.block_on(async {
        match backend {
            MetadataBackend::Redb => {
                let meta = open_local_meta_redb(data_dir)?;
                local_store_put(&meta, &chunks, key, &data, chunk_size, durability).await
            }
            #[cfg(feature = "tikv")]
            MetadataBackend::Tikv => {
                let meta = open_tikv_meta().await?;
                local_store_put(&meta, &chunks, key, &data, chunk_size, durability).await
            }
        }
    })
}

/// Store an object through the local-disk path, allocating the inode from the
/// persisted `meta:next_inode` counter and driving [`write::write_new_object`].
/// Generic over `M: MetadataStore` so the redb (dev) and TiKV (prod) backends run
/// the **identical** composition — the swap is selection, not a refactor.
async fn local_store_put<M: MetadataStore>(
    meta: &M,
    chunks: &FsChunkStore,
    key: &str,
    data: &[u8],
    chunk_size: usize,
    durability: EcScheme,
) -> Result<ExitCode, BoxError> {
    let inode_id = alloc_inode(meta).await?;
    let next_id = chunk_id_minter(inode_id);
    let outcome = write::write_new_object(
        meta,
        chunks,
        ROOT,
        key,
        inode_id,
        data,
        chunk_size,
        durability,
        NOW_MILLIS,
        LEASE_TTL_MILLIS,
        next_id,
    )
    .await?;

    match outcome {
        CommitOutcome::Committed => {
            let chunks = data.len().div_ceil(chunk_size.max(1));
            println!(
                "put ok: key={key} inode={inode_id} chunks={chunks} bytes={} durability={} version=1",
                data.len(),
                durability_label(durability),
            );
            Ok(ExitCode::SUCCESS)
        }
        CommitOutcome::Conflict => {
            eprintln!("wyrd: key `{key}` already exists");
            Ok(ExitCode::FAILURE)
        }
    }
}

/// `wyrd get <key> [--out <file>]`: read the object back, to a file or stdout.
fn cmd_get(args: &[String]) -> Result<ExitCode, BoxError> {
    let parsed = ParsedArgs::parse(args)?;
    let key = parsed
        .positional(0)
        .ok_or("get: a <key> argument is required")?;
    let data_dir = parsed.flag("data-dir").unwrap_or(DEFAULT_DATA_DIR);
    let out = parsed.flag("out");

    let backend = resolve_backend(&parsed)?;

    // Cluster client mode: reconstruct the object from fragments read back over
    // gRPC from the configured D servers (any-k arrive-first).
    if let Some(raw) = parsed.flag("endpoints") {
        let endpoints = parse_endpoints(raw)?;
        return cluster_get(backend, data_dir, &endpoints, key, out);
    }

    let chunks = open_local_chunks(data_dir)?;
    let runtime = tokio_runtime()?;
    runtime.block_on(async {
        match backend {
            MetadataBackend::Redb => {
                let meta = open_local_meta_redb(data_dir)?;
                local_store_get(&meta, &chunks, key, out).await
            }
            #[cfg(feature = "tikv")]
            MetadataBackend::Tikv => {
                let meta = open_tikv_meta().await?;
                local_store_get(&meta, &chunks, key, out).await
            }
        }
    })
}

/// Read an object back through the local-disk path, reconstructing it from the
/// on-disk chunk store via [`read::read_path`]. Generic over `M: MetadataStore` so
/// the redb (dev) and TiKV (prod) backends run the identical read composition.
async fn local_store_get<M: MetadataStore>(
    meta: &M,
    chunks: &FsChunkStore,
    key: &str,
    out: Option<&str>,
) -> Result<ExitCode, BoxError> {
    match read::read_path(meta, chunks, ROOT, key).await? {
        Some(bytes) => {
            match out {
                Some(path) => std::fs::write(path, &bytes)
                    .map_err(|e| format!("get: cannot write output file `{path}`: {e}"))?,
                None => std::io::stdout().write_all(&bytes)?,
            }
            Ok(ExitCode::SUCCESS)
        }
        None => {
            eprintln!("wyrd: key `{key}` not found");
            Ok(ExitCode::FAILURE)
        }
    }
}

/// `wyrd d-server`: host the local filesystem `ChunkStore` over the gRPC
/// `ChunkStore` service, registering the endpoint for discovery through the
/// `Coordination` seam, until Ctrl-C.
///
/// The coordination backend here is the process-local in-memory concrete, so a
/// D server in a separate process is not yet discoverable by a separate gateway
/// process — that awaits an etcd (or static-endpoint) backing behind the same
/// trait (ADR-0006), a composition swap, not M2.3 work. The cross-process and
/// multi-server discovery semantics are proven in-process by the role's tests.
fn cmd_d_server(args: &[String]) -> Result<ExitCode, BoxError> {
    let parsed = ParsedArgs::parse(args)?;
    let data_dir = parsed.flag("data-dir").unwrap_or(DEFAULT_DATA_DIR);
    let group = parsed.flag("group").unwrap_or(dserver::DSERVER_GROUP);
    let bind: SocketAddr = parsed
        .flag("bind")
        .unwrap_or(DEFAULT_DSERVER_BIND)
        .parse()
        .map_err(|e| format!("d-server: invalid --bind address: {e}"))?;
    let lease_ttl = Duration::from_secs(parse_u64_flag(
        &parsed,
        "lease-ttl-secs",
        DEFAULT_DSERVER_LEASE_TTL_SECS,
    )?);
    let renew_interval = Duration::from_secs(parse_u64_flag(
        &parsed,
        "renew-secs",
        DEFAULT_DSERVER_RENEW_SECS,
    )?);
    // The stable D-server id and opaque failure-domain label this server publishes
    // through registration (proposal 0005, "The placement record"). The label is what
    // lets the write selector place a chunk's fragments across distinct domains.
    let dserver_id = parse_u64_flag(&parsed, "id", 0)?;
    let failure_domain = parsed
        .flag("failure-domain")
        .unwrap_or(dserver::DEFAULT_FAILURE_DOMAIN)
        .to_string();

    // Admission control / backpressure (architecture §8.9): operator-tunable so the
    // server-wide limit matches the backing device's useful queue depth (shallow for
    // an HDD spindle, deep for SSD/NVMe) rather than a fixed constant. Unset flags
    // fall back to `AdmissionControl::default()`.
    let admission = dserver::AdmissionControl {
        max_concurrent_requests: parse_u64_flag(
            &parsed,
            "max-concurrent-requests",
            dserver::DEFAULT_MAX_CONCURRENT_REQUESTS as u64,
        )? as usize,
        request_timeout: Duration::from_secs(parse_u64_flag(
            &parsed,
            "request-timeout-secs",
            dserver::DEFAULT_REQUEST_TIMEOUT.as_secs(),
        )?),
        ..dserver::AdmissionControl::default()
    };

    let chunk_dir = Path::new(data_dir).join("chunks");
    let store = FsChunkStore::open(&chunk_dir)?;

    // The L5 coordination backend is selected by config, byte-for-byte the same as
    // the metadata backend (ADR-0008/0016): the process-local in-memory concrete by
    // default, the etcd concrete (cross-process discovery) under `--features etcd`.
    let coordination = resolve_coordination_backend(&parsed)?;

    // The d-server role is async (it hosts a tonic server); spin a tokio runtime
    // for it. The CLI's other commands stay sync (pollster) — only this role
    // needs a reactor.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let params = DServerParams {
        bind,
        dserver_id,
        failure_domain,
        admission,
        group: group.to_string(),
        lease_ttl,
        renew_interval,
        data_dir: data_dir.to_string(),
    };
    runtime.block_on(async move {
        match coordination {
            CoordinationBackend::Mem => {
                run_d_server(Arc::new(MemCoordination::new()), store, params).await
            }
            #[cfg(feature = "etcd")]
            CoordinationBackend::Etcd => {
                run_d_server(Arc::new(open_etcd_coordination().await?), store, params).await
            }
        }
    })?;
    Ok(ExitCode::SUCCESS)
}

/// The d-server parameters resolved from config, so [`run_d_server`] stays generic
/// over the coordination concrete without a long argument list.
struct DServerParams {
    bind: SocketAddr,
    dserver_id: u64,
    failure_domain: String,
    admission: dserver::AdmissionControl,
    group: String,
    lease_ttl: Duration,
    renew_interval: Duration,
    data_dir: String,
}

/// Host the local filesystem `ChunkStore` over gRPC and register through the given
/// `Coordination` concrete, until Ctrl-C. Generic over the coordination backend so
/// selecting in-memory vs etcd is "pass a different concrete", not a code fork —
/// the trait-level composition the ADR-0006 second implementation exists to prove.
async fn run_d_server<Co>(
    coord: Arc<Co>,
    store: FsChunkStore,
    params: DServerParams,
) -> Result<(), BoxError>
where
    Co: wyrd_traits::Coordination + 'static,
{
    let server = DServer::bind(store, params.bind)
        .await?
        .with_identity(params.dserver_id, params.failure_domain)
        .with_admission_control(params.admission);
    eprintln!(
        "wyrd d-server: serving gRPC ChunkStore on {} (data-dir {})",
        server.endpoint(),
        params.data_dir
    );
    let lease = server
        .register(&*coord, &params.group, params.lease_ttl)
        .await?;
    eprintln!(
        "wyrd d-server: registered under `{}` (lease {}); Ctrl-C to stop",
        params.group, lease.id
    );
    server
        .serve(coord, lease, params.renew_interval, async {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("wyrd d-server: shutting down");
        })
        .await
}

/// Parse an optional `--<name> <u64>` flag, defaulting when absent.
fn parse_u64_flag(parsed: &ParsedArgs, name: &str, default: u64) -> Result<u64, BoxError> {
    match parsed.flag(name) {
        Some(s) => s.parse().map_err(|_| {
            format!("d-server: invalid --{name} `{s}` (expected a whole number)").into()
        }),
        None => Ok(default),
    }
}

/// `wyrd demo`: the M0.8 round trip against in-memory backends — the zero-setup
/// "does the walking skeleton work?" smoke check.
fn cmd_demo() -> Result<ExitCode, BoxError> {
    let chunk_dir = std::env::temp_dir().join(format!("wyrd-demo-{}", std::process::id()));
    let gateway = Gateway::new(
        RedbMetadataStore::in_memory()?,
        FsChunkStore::open(&chunk_dir)?,
        MemCoordination::new(),
    );

    let result = block_on(async {
        gateway.announce("node-1").await?;
        let key = "demo/hello";
        let data = b"hello, wyrd";
        gateway.put_object(key, data).await?;
        let got = gateway.get_object(key).await?;
        if got.as_deref() != Some(&data[..]) {
            return Err::<(), BoxError>("PUT/GET was not byte-identical".into());
        }
        println!(
            "wyrd: S3 PUT/GET round-trip ok ({} bytes, {} node(s))",
            data.len(),
            gateway.nodes().await?.len()
        );
        Ok(())
    });

    let _ = std::fs::remove_dir_all(&chunk_dir);
    result?;
    Ok(ExitCode::SUCCESS)
}

/// Open the on-disk chunk store under `data_dir`, creating the dir if needed. The
/// metadata store is opened separately so the backend can be selected by config.
fn open_local_chunks(data_dir: &str) -> Result<FsChunkStore, BoxError> {
    let dir = Path::new(data_dir);
    std::fs::create_dir_all(dir)?;
    let chunks = FsChunkStore::open(dir.join("chunks"))?;
    Ok(chunks)
}

/// Open the local redb metadata store under `data_dir`, creating the dir if
/// needed — the dev-default backend for the local-disk path (ADR-0014).
fn open_local_meta_redb(data_dir: &str) -> Result<RedbMetadataStore, BoxError> {
    let dir = Path::new(data_dir);
    std::fs::create_dir_all(dir)?;
    let meta = RedbMetadataStore::open(dir.join("meta.redb"))?;
    Ok(meta)
}

/// Atomically allocate the next inode id from the persisted `meta:next_inode`
/// counter (default 1). On a lost race the commit returns `Conflict`; this retries
/// with capped exponential backoff, bounded by a **wall-clock budget**
/// ([`ALLOC_INODE_BUDGET`]) rather than a fixed attempt count, and gives up with an
/// `Err` only once that budget is spent — over distributed TiKV every attempt is a
/// network round-trip (proposal 0015 §"Composition, not refactor" item 3). A budget
/// (not a count) means a *contended* allocation on the single hot counter key keeps
/// retrying until it wins, so a normal burst of N concurrent creates is never rejected
/// as a "persistent conflict" just for exceeding some fixed N (Codex P2, #427).
/// Generic over `M: MetadataStore` so the dev (redb) and prod (TiKV) backends share
/// the identical allocator.
///
/// Public so the backend-selection regression can drive this exact production
/// helper generically (the mock `MetadataStore` is only constructible because the
/// helper is parameterized over `M` — the test load-bears the seam).
pub async fn alloc_inode<M: MetadataStore>(meta: &M) -> Result<u64, BoxError> {
    let deadline = tokio::time::Instant::now() + ALLOC_INODE_BUDGET;
    let mut attempt: u32 = 0;
    loop {
        let current = meta.get(NEXT_INODE_KEY).await?;
        let id: u64 = match &current {
            Some(bytes) => std::str::from_utf8(bytes)?.parse()?,
            None => 1,
        };
        let guard = match &current {
            Some(bytes) => WriteBatch::new().require(NEXT_INODE_KEY.to_vec(), bytes.clone()),
            None => WriteBatch::new().require_absent(NEXT_INODE_KEY.to_vec()),
        };
        let batch = guard.put(NEXT_INODE_KEY.to_vec(), (id + 1).to_string().into_bytes());
        if meta.commit(batch).await? == CommitOutcome::Committed {
            return Ok(id);
        }
        // Lost the race — back off (capped), but give up once a backoff would run
        // past the budget: that is a backend that genuinely cannot make progress,
        // not ordinary contention.
        let backoff = ALLOC_INODE_BASE_BACKOFF
            .saturating_mul(1u32 << attempt.min(ALLOC_INODE_MAX_SHIFT))
            .min(ALLOC_INODE_MAX_BACKOFF);
        if tokio::time::Instant::now() + backoff >= deadline {
            break;
        }
        tokio::time::sleep(backoff).await;
        attempt = attempt.saturating_add(1);
    }
    Err(format!(
        "alloc_inode: could not allocate an inode within {ALLOC_INODE_BUDGET:?} of contention (persistent metadata conflict)"
    )
    .into())
}

/// Mint chunk ids `inode_id << 64 | seq` — unique across objects and stable
/// across processes.
fn chunk_id_minter(inode_id: u64) -> impl FnMut() -> ChunkId {
    let mut seq: u128 = 0;
    move || {
        let id = ((inode_id as u128) << 64) | seq;
        seq += 1;
        id
    }
}

// --- Static-endpoints gateway client mode (M2.8, issue #155, proposal 0004) ---
//
// The user-facing way to drive a local distributed cluster: `wyrd put`/`get
// --endpoints <list>` fans each object's erasure-coded fragments out over gRPC to
// a *configured* list of networked D servers (no discovery backend — etcd /
// dynamic discovery is M3, ADR-0006), holding the metadata and the persisted
// inode allocator locally under `--data-dir`.
//
// It deliberately reuses the SAME id machinery as the local-disk path
// (`alloc_inode` + `chunk_id_minter`, above): the inode comes from the persisted
// `meta:next_inode` counter and the chunk ids are derived from it. That is what
// lets several distinct objects PUT across *separate* invocations over one
// `--data-dir` each get a distinct, persisted inode and non-colliding chunk ids —
// an in-process counter (reset every process) would re-allocate inode 1 on the
// second PUT (a bogus "concurrent writer won" conflict) and reuse chunk id 1,
// clobbering the first object's fragments on the shared chunk store.

/// The chunk plane of the static-endpoints gateway client mode: the M2 fan-out
/// placement store ([`FanoutChunkStore`]) over one gRPC [`GrpcChunkStore`] client
/// per configured D-server endpoint. Fragment index `i` lands on D server `i % n`
/// (`fanout.rs`), so a chunk's fragments prefer distinct, real, networked D
/// servers.
pub type GrpcFanout = FanoutChunkStore<GrpcChunkStore>;

/// Split a comma-separated `--endpoints` value (e.g.
/// `http://127.0.0.1:50051,http://127.0.0.1:50052`) into dialable endpoint
/// strings, rejecting an empty list — a fan-out with no backend can place no
/// fragment.
pub fn parse_endpoints(raw: &str) -> Result<Vec<String>, BoxError> {
    let endpoints: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if endpoints.is_empty() {
        return Err(
            "--endpoints needs at least one D-server URL (e.g. http://127.0.0.1:50051)".into(),
        );
    }
    Ok(endpoints)
}

/// Dial each endpoint as a gRPC `ChunkStore` client and compose them into the M2
/// fan-out placement store. Connecting up front surfaces an unreachable D server
/// as a clear startup error rather than a mid-write failure.
pub async fn connect_fanout(endpoints: &[String]) -> Result<GrpcFanout, BoxError> {
    if endpoints.is_empty() {
        return Err("connect_fanout: at least one endpoint is required".into());
    }
    let mut clients = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let client = GrpcChunkStore::connect(endpoint.clone())
            .await
            .map_err(|e| format!("gateway: cannot connect to D server `{endpoint}`: {e}"))?;
        clients.push(client);
    }
    Ok(FanoutChunkStore::new(clients))
}

/// Open the local metadata store (redb) under `data_dir` for the gateway client
/// mode, creating the dir if needed. Unlike [`open_local_meta_redb`], the cluster
/// path holds **no** local chunk store — every fragment crosses the wire to a
/// configured D server — but it keeps the metadata store and its persisted inode
/// allocator (`meta:next_inode`) locally, exactly as the local-disk path does.
pub fn open_cluster_meta(data_dir: &str) -> Result<RedbMetadataStore, BoxError> {
    let dir = Path::new(data_dir);
    std::fs::create_dir_all(dir)?;
    let meta = RedbMetadataStore::open(dir.join("meta.redb"))?;
    Ok(meta)
}

/// Store an object through the gateway client mode over `chunks`, allocating the
/// inode from the persisted `meta:next_inode` counter and deriving chunk ids from
/// it — exactly the local-disk path's [`write::write_new_object`] composition,
/// with the on-disk chunk store swapped for the gRPC fan-out. Persisting the ids
/// is what makes storing several distinct objects across separate invocations
/// (fresh process / new composition over the same `data_dir`) work.
pub async fn cluster_store_put<M: MetadataStore, C: PlacementChunkStore>(
    meta: &M,
    chunks: &C,
    key: &str,
    data: &[u8],
    chunk_size: usize,
    durability: EcScheme,
) -> Result<CommitOutcome, BoxError> {
    let inode_id = alloc_inode(meta).await?;
    let next_id = chunk_id_minter(inode_id);
    let outcome = write::write_new_object(
        meta,
        chunks,
        ROOT,
        key,
        inode_id,
        data,
        chunk_size,
        durability,
        NOW_MILLIS,
        LEASE_TTL_MILLIS,
        next_id,
    )
    .await?;
    Ok(outcome)
}

/// Read an object back through the gateway client mode, reconstructing it from
/// fragments read over `chunks` — the same [`read::read_path`] the local-disk
/// path uses, over the gRPC fan-out.
pub async fn cluster_store_get<M: MetadataStore, C: PlacementChunkStore>(
    meta: &M,
    chunks: &C,
    key: &str,
) -> Result<Option<Vec<u8>>, BoxError> {
    read::read_path(meta, chunks, ROOT, key).await
}

/// Build the multi-threaded tokio runtime the CLI's store paths need: the gRPC
/// clients (tonic) and the production TiKV client are both async and `tokio`-bound
/// — the local-disk redb path shares the same runtime rather than
/// `pollster::block_on` so any backend runs identically (proposal 0015
/// §"Composition, not refactor" item 2).
fn tokio_runtime() -> Result<tokio::runtime::Runtime, BoxError> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

/// `wyrd s3`: the **runnable gateway server role** — serve the S3-compatible HTTP wire
/// surface (bucket-scoped object PUT/GET/DELETE with mandatory SigV4, streaming bodies)
/// over the composed redb + filesystem + in-memory-coordination backends (issue #364,
/// m4-first-deployment-blueprint:59). This is the "Stateless S3 front door" the
/// first-deployment gate (#367) runs; the public-TLS terminator and the multi-node,
/// discovery-driven stand-up are gated on the separate coordination prerequisite
/// (proposal 0015 §"Deployment prerequisite", 443-463), so at M4 this binds plaintext
/// loopback/host and an operator fronts it with the public S3 cert.
fn cmd_s3(args: &[String]) -> Result<ExitCode, BoxError> {
    let parsed = ParsedArgs::parse(args)?;
    let listen: SocketAddr = parsed
        .flag("s3-listen")
        .unwrap_or(DEFAULT_S3_LISTEN)
        .parse()
        .map_err(|e| format!("s3: invalid --s3-listen address: {e}"))?;
    let data_dir = parsed.flag("data-dir").unwrap_or(DEFAULT_DATA_DIR);
    let region = parsed
        .flag("region")
        .unwrap_or(DEFAULT_S3_REGION)
        .to_string();
    let access_key = parsed
        .flag("access-key")
        .map(str::to_string)
        .or_else(|| std::env::var("WYRD_S3_ACCESS_KEY").ok())
        .ok_or(
            "s3: --access-key (or WYRD_S3_ACCESS_KEY) is required; there is no anonymous access",
        )?;
    let secret_key = parsed
        .flag("secret-key")
        .map(str::to_string)
        .or_else(|| std::env::var("WYRD_S3_SECRET_KEY").ok())
        .ok_or("s3: --secret-key (or WYRD_S3_SECRET_KEY) is required")?;

    let dir = Path::new(data_dir);
    std::fs::create_dir_all(dir)?;
    let meta = RedbMetadataStore::open(dir.join("meta.redb"))?;
    let chunks = FsChunkStore::open(dir.join("chunks"))?;
    let gateway = Arc::new(Gateway::new(meta, chunks, MemCoordination::new()));

    let mut config = s3::S3Config::new(vec![s3::sigv4::Credentials {
        access_key_id: access_key,
        secret_access_key: secret_key,
    }]);
    config.region = region.clone();
    let server = s3::S3Gateway::new(Arc::clone(&gateway), config);

    let runtime = tokio_runtime()?;
    runtime.block_on(async move {
        // Resume the in-process id allocators above everything already persisted under
        // `data_dir`, so a RESTART over an existing store never reuses a committed inode or
        // chunk id and corrupts an existing object (issue #364 durability finding 1).
        gateway.recover().await?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        eprintln!(
            "wyrd s3: serving S3-compatible HTTP on {} (data-dir {data_dir})",
            listener.local_addr()?
        );
        eprintln!(
            "wyrd s3: SigV4 required (region {region}, service s3); no anonymous access. Ctrl-C to stop"
        );
        server.serve(listener).await?;
        Ok::<(), BoxError>(())
    })?;
    Ok(ExitCode::SUCCESS)
}

/// `wyrd put … --endpoints <list>`: store the object through the static-endpoints
/// gateway client mode — fragments fan out over gRPC to the configured D servers,
/// metadata (and the persisted inode allocator) held locally under `data_dir`.
fn cluster_put(
    backend: MetadataBackend,
    data_dir: &str,
    endpoints: &[String],
    key: &str,
    data: &[u8],
    chunk_size: usize,
    durability: EcScheme,
) -> Result<ExitCode, BoxError> {
    let runtime = tokio_runtime()?;
    runtime.block_on(async {
        let fanout = connect_fanout(endpoints).await?;
        let outcome = match backend {
            MetadataBackend::Redb => {
                let meta = open_cluster_meta(data_dir)?;
                cluster_store_put(&meta, &fanout, key, data, chunk_size, durability).await?
            }
            #[cfg(feature = "tikv")]
            MetadataBackend::Tikv => {
                let meta = open_tikv_meta().await?;
                cluster_store_put(&meta, &fanout, key, data, chunk_size, durability).await?
            }
        };
        match outcome {
            CommitOutcome::Committed => {
                let chunks = data.len().div_ceil(chunk_size.max(1));
                println!(
                    "put ok (cluster): key={key} servers={} chunks={chunks} bytes={} durability={}",
                    endpoints.len(),
                    data.len(),
                    durability_label(durability),
                );
                Ok(ExitCode::SUCCESS)
            }
            CommitOutcome::Conflict => {
                eprintln!("wyrd: key `{key}` already exists");
                Ok(ExitCode::FAILURE)
            }
        }
    })
}

/// `wyrd get … --endpoints <list>`: read the object back through the
/// static-endpoints gateway client mode, reconstructing it from fragments read
/// over gRPC from the configured D servers.
fn cluster_get(
    backend: MetadataBackend,
    data_dir: &str,
    endpoints: &[String],
    key: &str,
    out: Option<&str>,
) -> Result<ExitCode, BoxError> {
    let runtime = tokio_runtime()?;
    runtime.block_on(async {
        let fanout = connect_fanout(endpoints).await?;
        let found = match backend {
            MetadataBackend::Redb => {
                let meta = open_cluster_meta(data_dir)?;
                cluster_store_get(&meta, &fanout, key).await?
            }
            #[cfg(feature = "tikv")]
            MetadataBackend::Tikv => {
                let meta = open_tikv_meta().await?;
                cluster_store_get(&meta, &fanout, key).await?
            }
        };
        match found {
            Some(bytes) => {
                match out {
                    Some(path) => std::fs::write(path, &bytes)
                        .map_err(|e| format!("get: cannot write output file `{path}`: {e}"))?,
                    None => std::io::stdout().write_all(&bytes)?,
                }
                Ok(ExitCode::SUCCESS)
            }
            None => {
                eprintln!("wyrd: key `{key}` not found");
                Ok(ExitCode::FAILURE)
            }
        }
    })
}

/// Positional arguments plus `--flag value` pairs.
struct ParsedArgs {
    positionals: Vec<String>,
    flags: HashMap<String, String>,
}

impl ParsedArgs {
    fn parse(args: &[String]) -> Result<Self, BoxError> {
        let mut positionals = Vec::new();
        let mut flags = HashMap::new();
        let mut i = 0;
        while i < args.len() {
            if let Some(name) = args[i].strip_prefix("--") {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| format!("flag `--{name}` needs a value"))?;
                flags.insert(name.to_string(), value.clone());
                i += 2;
            } else {
                positionals.push(args[i].clone());
                i += 1;
            }
        }
        Ok(Self { positionals, flags })
    }

    fn positional(&self, index: usize) -> Option<&str> {
        self.positionals.get(index).map(String::as_str)
    }

    fn flag(&self, name: &str) -> Option<&str> {
        self.flags.get(name).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `parse_durability` — `:95` `==`/`||` and `:110` `k == 0`. Pin every form:
    /// the two `none` aliases, a real `rs(k,m)`, the `k == 0` refusal, and a
    /// non-form. `!=`/`&&` on the alias test, or `!=` on the `k == 0` guard, each
    /// flip one of these outcomes.
    #[test]
    fn parse_durability_maps_each_form() {
        assert_eq!(parse_durability("none").unwrap(), EcScheme::None);
        assert_eq!(parse_durability("replication(1)").unwrap(), EcScheme::None);
        assert_eq!(
            parse_durability("rs(2,1)").unwrap(),
            EcScheme::ReedSolomon { k: 2, m: 1 }
        );
        assert!(parse_durability("rs(0,1)").is_err(), "k must be at least 1");
        assert!(parse_durability("nonsense").is_err());
    }

    /// `parse_endpoints` — `:413` `delete !`. The `!s.is_empty()` filter drops
    /// blank entries and KEEPS the rest; deleting the `!` keeps only the blanks, so
    /// every real endpoint list collapses to empty and errors.
    #[test]
    fn parse_endpoints_splits_trims_and_rejects_empty() {
        assert_eq!(
            parse_endpoints("http://a, http://b").unwrap(),
            vec!["http://a".to_string(), "http://b".to_string()]
        );
        assert_eq!(
            parse_endpoints("http://only").unwrap(),
            vec!["http://only".to_string()]
        );
        assert!(parse_endpoints("").is_err(), "no endpoints is an error");
        assert!(
            parse_endpoints("  ,  ").is_err(),
            "only-blank entries is an error"
        );
    }

    /// `ParsedArgs::parse` — `:585` `+= -> *=` on the flag branch's `i += 2`. A
    /// `--flag value` consumes BOTH tokens; `*=` (or the positional branch's
    /// `i *= 1`) leaves the value behind as a stray positional (or loops). Pin that
    /// a flag's value is not a positional.
    #[test]
    fn parsed_args_consumes_flag_value_pairs() {
        let args: Vec<String> = ["a", "--k", "v", "b"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let parsed = ParsedArgs::parse(&args).unwrap();
        assert_eq!(
            parsed.positionals,
            vec!["a".to_string(), "b".to_string()],
            "a flag consumes its value; the value is not a positional"
        );
        assert_eq!(parsed.flag("k"), Some("v"));
    }

    /// `chunk_id_minter` — `:375` `<<`. The id packs the inode into the high 64
    /// bits with the sequence in the low bits; `>>` drops the inode entirely.
    /// (The sibling `| -> ^` is an equivalent mutant — non-overlapping bit ranges
    /// — handled in the equivalent-mutant declaration.)
    #[test]
    fn chunk_id_minter_packs_inode_in_the_high_bits() {
        let mut mint = chunk_id_minter(5);
        assert_eq!(
            mint(),
            5u128 << 64,
            "first id: inode in the high bits, seq 0"
        );
        assert_eq!(mint(), (5u128 << 64) | 1, "seq increments in the low bits");
    }

    /// `:46` `<<` — the default chunk size is 1 MiB; `>>` collapses it to 0.
    #[test]
    fn default_chunk_size_is_one_mib() {
        assert_eq!(DEFAULT_CHUNK_SIZE, 1 << 20);
    }
}
