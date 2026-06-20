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
use std::path::Path;
use std::process::ExitCode;

use pollster::block_on;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{ChunkId, CommitOutcome, MetadataStore, WriteBatch};

use crate::Gateway;

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
    eprintln!("  wyrd put <file> --key <name> [--data-dir DIR] [--chunk-size N]");
    eprintln!("  wyrd get <key> [--out <file>] [--data-dir DIR]");
    eprintln!("  wyrd demo");
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

    let data =
        std::fs::read(file).map_err(|e| format!("put: cannot read input file `{file}`: {e}"))?;
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
            NOW_MILLIS,
            LEASE_TTL_MILLIS,
            next_id,
        )
        .await?;

        match outcome {
            CommitOutcome::Committed => {
                let chunks = data.len().div_ceil(chunk_size.max(1));
                println!(
                    "put ok: key={key} inode={inode_id} chunks={chunks} bytes={} version=1",
                    data.len()
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
