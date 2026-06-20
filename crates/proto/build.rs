//! Compile the `.proto` wire contracts into Rust with tonic, using protox as a
//! pure-Rust frontend so the build needs no system `protoc`. `compile_fds` takes
//! protox's descriptor set directly — `compile_protos` would shell out to
//! `protoc`, which we deliberately avoid (ADR-0016).

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let protos = ["proto/wyrd/v0/chunk.proto", "proto/wyrd/v0/commit.proto"];
    let includes = ["proto"];

    // Re-run codegen only when a proto file changes.
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let file_descriptors = protox::compile(protos, includes)?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(file_descriptors)?;
    Ok(())
}
