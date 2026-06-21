# Container image for a real, networked gRPC D server (Milestone 2 PR step 7).
#
# A multi-stage build so the produced binary is glibc-portable: it is compiled in
# the Rust image and run on the matching Debian runtime, with no dependency on the
# host's toolchain or libc (the host may be far newer than any base image). The
# `d-server` role is a subcommand of the single `wyrd` binary (ADR-0014,
# ADR-0016): it opens a local filesystem ChunkStore and serves it over the gRPC
# ChunkStore service. Used by `cargo xtask integration` /
# `tests/tier2_integration.rs`.

# Build stage — compile the wyrd binary against the image's pinned glibc.
# Pin the exact patch (`1.96.0`) to match `rust-toolchain.toml`, so the base tag
# floating to a newer patch can't trigger a build-time toolchain re-download.
FROM rust:1.96.0-bookworm AS build
WORKDIR /src
COPY . .
# `--locked` so the container build uses the committed `Cargo.lock` exactly,
# matching the gated workspace rather than silently resolving newer deps.
RUN cargo build --release --locked --bin wyrd

# Runtime stage — a minimal image hosting just the d-server role.
FROM debian:bookworm-slim
COPY --from=build /src/target/release/wyrd /usr/local/bin/wyrd
# gRPC listen port; the compose file publishes it on an ephemeral host port.
EXPOSE 50051
ENTRYPOINT ["wyrd"]
