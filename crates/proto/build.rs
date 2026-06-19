//! Compile the `.proto` wire contracts into Rust with prost, using protox as a
//! pure-Rust frontend so the build needs no system `protoc`.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let protos = ["proto/wyrd/v0/chunk.proto", "proto/wyrd/v0/commit.proto"];
    let includes = ["proto"];

    // Re-run codegen only when a proto file changes.
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let file_descriptors = protox::compile(protos, includes)?;
    prost_build::Config::new().compile_fds(file_descriptors)?;
    Ok(())
}
