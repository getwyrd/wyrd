//! Compile the `.proto` wire contracts into Rust with tonic, using protox as a
//! pure-Rust frontend so the build needs no system `protoc` (ADR-0016).
//!
//! Two codegen backends, selected by the build flag (proposal 0004 "DST"):
//! - **normal build** — `tonic-prost-build`'s `compile_fds` takes protox's
//!   descriptor set directly and emits the real client/server stubs.
//! - **`--cfg madsim`** — `madsim-tonic-build` emits stubs against madsim's
//!   simulated transport (into `OUT_DIR/sim/`), so the *real* `GrpcChunkStore`
//!   wire code runs on madsim's deterministic network (ADR-0009). It has no
//!   `compile_fds`, so the descriptor set is written to a file and fed via
//!   `file_descriptor_set_path` + `skip_protoc_run` — still no system `protoc`.
//!
//! Cargo exports `CARGO_CFG_MADSIM` to this build script exactly when the crate
//! is being compiled under `--cfg madsim`, which is how the branch is chosen.

#![forbid(unsafe_code)]

use std::error::Error;
use std::path::PathBuf;

use prost::Message;

fn main() -> Result<(), Box<dyn Error>> {
    let protos = ["proto/wyrd/v0/chunk.proto", "proto/wyrd/v0/commit.proto"];
    let includes = ["proto"];

    // Re-run codegen only when a proto file changes.
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let file_descriptors = protox::compile(protos, includes)?;

    if std::env::var_os("CARGO_CFG_MADSIM").is_some() {
        let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
        let fds_path = out_dir.join("wyrd_v0_fds.bin");
        std::fs::write(&fds_path, file_descriptors.encode_to_vec())?;
        madsim_tonic_build::configure()
            .build_client(true)
            .build_server(true)
            .file_descriptor_set_path(&fds_path)
            .skip_protoc_run()
            .compile_protos(&protos, &includes)?;
    } else {
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .compile_fds(file_descriptors)?;
    }
    Ok(())
}
