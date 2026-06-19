//! The Wyrd single binary.
//!
//! Assembles the M0 walking skeleton in one process: it wires the concrete
//! backends (redb metadata, filesystem chunks, in-memory coordination) behind
//! the trait seams (ADR-0010) into the S3 [`Gateway`], then runs an S3 PUT →
//! four-phase commit → byte-identical GET round trip. Backend selection is this
//! composition, not config — a networked profile swaps concretes here.

#![forbid(unsafe_code)]

use std::error::Error;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;

type BoxError = Box<dyn Error + Send + Sync>;

fn main() -> Result<(), BoxError> {
    // One process, one machine (M0 is a proof, not a deployable product): an
    // embedded redb metadata store, filesystem chunks under a temp dir, and
    // in-memory coordination.
    let chunk_dir = std::env::temp_dir().join(format!("wyrd-demo-{}", std::process::id()));
    let gateway = Gateway::new(
        RedbMetadataStore::in_memory()?,
        FsChunkStore::open(&chunk_dir)?,
        MemCoordination::new(),
    );

    let result = pollster::block_on(async {
        gateway.announce("node-1").await?;

        let key = "demo/hello";
        let data = b"hello, wyrd";
        gateway.put_object(key, data).await?;
        let got = gateway.get_object(key).await?;
        if got.as_deref() != Some(&data[..]) {
            return Err("PUT/GET was not byte-identical".into());
        }
        println!(
            "wyrd: S3 PUT/GET round-trip ok ({} bytes, {} node(s))",
            data.len(),
            gateway.nodes().await?.len()
        );
        Ok::<(), BoxError>(())
    });

    let _ = std::fs::remove_dir_all(&chunk_dir);
    result
}
