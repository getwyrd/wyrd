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
# Drop root (issue #286): a dedicated unprivileged system user/group, and
# `/data` pre-created and owned by it so the compose service's
# `--data-dir /data` (docker-compose.yml:17) is writable once we switch to it
# below. `--system` skips password/home-dir setup nothing here needs; the
# fixed 10001:10001 keeps the id stable across rebuilds without depending on
# base-image UID allocation order.
RUN groupadd --system --gid 10001 dserver \
    && useradd --system --uid 10001 --gid dserver --no-create-home --shell /usr/sbin/nologin dserver \
    && mkdir -p /data \
    && chown dserver:dserver /data
# gRPC listen port; the compose file publishes it on an ephemeral host port.
EXPOSE 50051
# Non-root for the process the ENTRYPOINT starts (must come after the file
# ownership above and before ENTRYPOINT to actually apply to it).
USER dserver:dserver
ENTRYPOINT ["wyrd"]
