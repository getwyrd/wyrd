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
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, MetadataStore, PlacementChunkStore, WriteBatch,
};

use crate::dserver::{self, DServer};
use crate::{Gateway, DEFAULT_DURABILITY};

/// Default endpoint the `d-server` role binds and advertises.
const DEFAULT_DSERVER_BIND: &str = "127.0.0.1:50051";
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

/// Parse `args` (including argv[0]) and run the requested command, returning the
/// process exit code.
pub fn run(args: impl Iterator<Item = String>) -> ExitCode {
    let args: Vec<String> = args.skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("put") => cmd_put(&args[1..]),
        Some("get") => cmd_get(&args[1..]),
        Some("d-server") => cmd_d_server(&args[1..]),
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
    eprintln!("  wyrd put <file> --key <name> [--data-dir DIR] [--chunk-size N] [--durability rs(k,m)|none] [--endpoints URL,URL,…]");
    eprintln!("  wyrd get <key> [--out <file>] [--data-dir DIR] [--endpoints URL,URL,…]");
    eprintln!("  wyrd d-server [--bind ADDR] [--data-dir DIR] [--group NAME] [--lease-ttl-secs N] [--renew-secs N]");
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

    // Cluster client mode: fan the object's fragments out over gRPC to the
    // configured D servers, holding metadata locally under `data_dir`.
    if let Some(raw) = parsed.flag("endpoints") {
        let endpoints = parse_endpoints(raw)?;
        return cluster_put(data_dir, &endpoints, key, &data, chunk_size, durability);
    }

    let (meta, chunks) = open_backends(data_dir)?;

    block_on(async {
        let inode_id = alloc_inode(&meta).await?;
        let next_id = chunk_id_minter(inode_id);
        let outcome = write::write_new_object(
            &meta,
            &chunks,
            ROOT,
            key,
            inode_id,
            &data,
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
    })
}

/// `wyrd get <key> [--out <file>]`: read the object back, to a file or stdout.
fn cmd_get(args: &[String]) -> Result<ExitCode, BoxError> {
    let parsed = ParsedArgs::parse(args)?;
    let key = parsed
        .positional(0)
        .ok_or("get: a <key> argument is required")?;
    let data_dir = parsed.flag("data-dir").unwrap_or(DEFAULT_DATA_DIR);
    let out = parsed.flag("out");

    // Cluster client mode: reconstruct the object from fragments read back over
    // gRPC from the configured D servers (any-k arrive-first).
    if let Some(raw) = parsed.flag("endpoints") {
        let endpoints = parse_endpoints(raw)?;
        return cluster_get(data_dir, &endpoints, key, out);
    }

    let (meta, chunks) = open_backends(data_dir)?;

    block_on(async {
        match read::read_path(&meta, &chunks, ROOT, key).await? {
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

    let chunk_dir = Path::new(data_dir).join("chunks");
    let store = FsChunkStore::open(&chunk_dir)?;

    // The d-server role is async (it hosts a tonic server); spin a tokio runtime
    // for it. The CLI's other commands stay sync (pollster) — only this role
    // needs a reactor.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let coord = Arc::new(MemCoordination::new());
        let server = DServer::bind(store, bind).await?;
        eprintln!(
            "wyrd d-server: serving gRPC ChunkStore on {} (data-dir {data_dir})",
            server.endpoint()
        );
        let lease = server.register(&*coord, group, lease_ttl).await?;
        eprintln!(
            "wyrd d-server: registered under `{group}` (lease {}); Ctrl-C to stop",
            lease.id
        );
        server
            .serve(coord, lease, renew_interval, async {
                let _ = tokio::signal::ctrl_c().await;
                eprintln!("wyrd d-server: shutting down");
            })
            .await
    })?;
    Ok(ExitCode::SUCCESS)
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

/// Open the on-disk backends under `data_dir`, creating it if needed.
fn open_backends(data_dir: &str) -> Result<(RedbMetadataStore, FsChunkStore), BoxError> {
    let dir = Path::new(data_dir);
    std::fs::create_dir_all(dir)?;
    let meta = RedbMetadataStore::open(dir.join("meta.redb"))?;
    let chunks = FsChunkStore::open(dir.join("chunks"))?;
    Ok((meta, chunks))
}

/// Atomically allocate the next inode id from the persisted `meta:next_inode`
/// counter (default 1). Retries if a concurrent writer bumped it first.
async fn alloc_inode(meta: &RedbMetadataStore) -> Result<u64, BoxError> {
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
    }
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
/// mode, creating the dir if needed. Unlike [`open_backends`], the cluster path
/// holds **no** local chunk store — every fragment crosses the wire to a
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
pub async fn cluster_store_put<C: ChunkStore>(
    meta: &RedbMetadataStore,
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
pub async fn cluster_store_get<C: PlacementChunkStore>(
    meta: &RedbMetadataStore,
    chunks: &C,
    key: &str,
) -> Result<Option<Vec<u8>>, BoxError> {
    read::read_path(meta, chunks, ROOT, key).await
}

/// Build the multi-threaded tokio runtime the cluster client mode needs: the gRPC
/// clients are async (tonic), unlike the sync local-disk paths (pollster).
fn cluster_runtime() -> Result<tokio::runtime::Runtime, BoxError> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

/// `wyrd put … --endpoints <list>`: store the object through the static-endpoints
/// gateway client mode — fragments fan out over gRPC to the configured D servers,
/// metadata (and the persisted inode allocator) held locally under `data_dir`.
fn cluster_put(
    data_dir: &str,
    endpoints: &[String],
    key: &str,
    data: &[u8],
    chunk_size: usize,
    durability: EcScheme,
) -> Result<ExitCode, BoxError> {
    let runtime = cluster_runtime()?;
    runtime.block_on(async {
        let meta = open_cluster_meta(data_dir)?;
        let fanout = connect_fanout(endpoints).await?;
        match cluster_store_put(&meta, &fanout, key, data, chunk_size, durability).await? {
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
    data_dir: &str,
    endpoints: &[String],
    key: &str,
    out: Option<&str>,
) -> Result<ExitCode, BoxError> {
    let runtime = cluster_runtime()?;
    runtime.block_on(async {
        let meta = open_cluster_meta(data_dir)?;
        let fanout = connect_fanout(endpoints).await?;
        match cluster_store_get(&meta, &fanout, key).await? {
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
